#!/usr/bin/perl

# When migrating data from old snapp to tsds, first a new interface meta-data record ("measurement") is entered, 
# which has 'start' = NOW. 
# Data starts being added to the data collection with that identifier. 
# At some point, the meta-data document is "replaced" by one with more data which is gotten from the grnoc db.
# Also at some point, old data documents are added to tsds, copied from old-snapp. 
# Once all the old data is copied over, we need to run a script to change the meta-data's 'start' value to the time 
# that the (old) data really started.
# 
# Change the start time of the FIRST meta-data ("measurement") document to match that of the first data document.
# 
# Since part of the shard key is the start and end time for the meta-data, we can't just change the value. 
# We have to read the metadata into memory, change the value, delete the old record in mongo, insert the new one.
# 
# ---------------------- 
# To run:  perl tsds_fix_start_date.pl -db DATABASW -query 'QUERY'
#    where DATABASE is the db (eg, interface) and QUERY is a tsds query to get all the documents you want to work on.
#    eg, perl tsds_fix_start_date.pl --db interface --query '{"network": "N-Wave", "node_role": "acpdu"}' 
#                                    --logging logging.conf --debug
# 
# See below for config and logging files required
#
# Run with -debug to print identifiers and start times (screen and log file)
# Run with -no-change to just print identifiers and msgs to screen about what it would do (nothing if nothing) 
#  (just totals in log file)
# Run with neither of these options for real runs (will just print total counts to both screen and log file, 
#  and $cnt (every 100) to screen,
# Errors will always be printed to screen and log file.
# ---------------------- 

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';
use lib './venv/lib/perl5';

use Data::Dumper;
use GRNOC::Log;
use Getopt::Long;
use GRNOC::Config;
use GRNOC::CLI;
use MongoDB;
use JSON::XS;

use Date::Parse;

use constant DEFAULT_CONFIG_FILE => '/etc/grnoc/tsds/services/config.xml';
use constant DEFAULT_LOGGING_FILE => '/etc/grnoc/tsds/services/logging.conf';
use constant DEFAULT_LOCK_DIR => '/var/run/grnoc/tsds/services/';

### command line options ###
my $config_file = DEFAULT_CONFIG_FILE;
my $logging_conf = DEFAULT_LOGGING_FILE;
my $database;                 # eg, interface
my $query;                    # put in single quotes - eg, '{"network: "N-Wave", "node_role": "acpdu"}'
my $debug;                    # print details for each identfier 
my $no_change;                # don't actually delete or insert anything; just print what would happen
my $help;

# parse options from command line
GetOptions( "help|h|?" => \$help,
            "debug" => \$debug,
            "no-change" => \$no_change,
            "config=s" => \$config_file,
            "logging=s" => \$logging_conf,
            "db=s" => \$database,
            "query=s" => \$query );

# did they ask for help?
usage() if ( $help );

# did they specify a db and query?
usage() if ( !$database or !$query );

# does the config file exist?
if (! $config_file || ! -e $config_file){
    print "Missing or invalid config file: $config_file\n";
    print USAGE . "\n";
    exit;
}

# for no-change mode, also turn on debug
$debug = 1 if ($no_change);

# logging 
GRNOC::Log->new( config => $logging_conf );

log_info("DATABASE = $database, QUERY = $query");

# The query specifies which interfaces, or whatever, to process.
# decode the query (changes it from a string into a hash)
my $json = JSON::XS->new();
eval {
    $query = $json->decode( $query );
    };
if ($@) {
    log_error( "ERROR: There was a problem decoding the query: $@ " );
    exit;
}

# read config file, connect to mongo; get collections
my $mongo = _mongo_connect();
my $db       = $mongo->get_database($database);
my $info_collection = $db->get_collection("measurements");
my $data_collection = $db->get_collection("data");

# get the distinct identifiers of the "measurement" documents to process 
my $identifiers_to_do;
eval {
     $identifiers_to_do = $db->run_command([ "distinct" => "measurements",
                                            "key" => "identifier",
                                            "query" => $query ]);
    };
if ($@) {
    log_error( "ERROR: There was a problem getting the identifiers to do: $@ " );
    exit;
}

my @identifiers =  @{$identifiers_to_do->{'values'}} ;
if (!@identifiers) {
    log_error( "ERROR: Identifiers array is undefined" );
    exit;
}
my $nids = @identifiers;
log_info("NO. OF INDENTIFIERS = ". $nids);

my $ndeletes = 0;
my $ninserts = 0;

# progress bar
my $cnt = 0;
my $cli = GRNOC::CLI->new();
$cli->start_progress( $nids);

foreach my $ident ( @identifiers ) {
    $cnt++;
    $cli->update_progress($cnt);


    # Get (only) the start time of the earliest data with the current identifer 
    # (sort by 'start' ascending; ->all makes it return an array; get first/only array element)
    my $first_data;
    eval {
        $first_data = ($data_collection->find({ identifier => $ident }) 
                                      ->fields({ "start" => 1 })
                                      ->sort({ start => 1 })
                                      ->limit(1)->all)[0]; 
    };
    if ($@) {
        log_error("Identifier: $ident");
        log_error( "ERROR: There was a problem getting the first data start time: $@ " );
        exit;
    }

    my $data_start_time = $first_data->{"start"};
    if ( !defined($data_start_time) ) {
        log_error("Identifier: $ident");
        log_error( "ERROR: the  first data start time is undefined!?\n" );
        exit;
    }

    # Get the (whole) earliest info/measurement doc with the current identifier 
    # (sort by 'start' ascending; ->all makes it return an array; get first/only array element)
    my $first_info; 
    eval {
        $first_info  = ($info_collection->find({ identifier => $ident })
                                        ->sort({ start => 1 })
                                        ->limit(1)->all)[0]; 
    };
    if ($@) {
        log_error("Identifier: $ident");
        log_error( "ERROR: There was a problem getting the earliest measurement doc: $@ " );
        exit;
    }
   
    my $first_info_start_time = $first_info->{"start"};
    if ( !defined($first_info_start_time) ) {
        log_error("Identifier: $ident");
        log_error( "ERROR: the first info start time is undefined!?\n" );
        exit;
    }
    
    my $time_diff = $data_start_time - $first_info_start_time;
    log_debug("identifier = $ident");
    log_debug("    data-start = $data_start_time,  info-start = $first_info_start_time,  diff = $time_diff ");

    # If the data starts before the info, change the date on the info to match.
    if ($data_start_time >= $first_info_start_time) {
        next;
    }

    # counters for no_change mode or for real
    $ndeletes++;
    $ninserts++;

    if ($no_change) {
        log_debug("  WOULD DELETE _id = $first_info->{'_id'}"); 
    } 
    else {
        # try to remove the first info/measurement doc 
        # (must delete and insert rather than just modify due to the shard keys) 
        eval {
            log_debug("  DELETING _id = $first_info->{'_id'}");
            $info_collection->remove({ "_id" => $first_info->{"_id"} }); 
        };
        if($@){
            # if there was a problem, don't attempt to insert an updated doc
            log_error( "  **** ERROR: Problem deleting measurement document: $@ " );
            log_error( "  DOCUMENT TO BE DELETED: ".Dumper($first_info) );
            $ndeletes--;
            exit;
        }
    }

    # now update the start time to match the start time of the first data and delete the id in the hash
    $first_info->{'start'} = $data_start_time;
    delete $first_info->{'_id'};

    if ($no_change) {
        log_debug("  WOULD INSERT with start = $first_info->{'start'}"); 
    } 
    else {
        # try to insert the modified doc  
        eval {
            log_debug("  INSERTING start = $first_info->{'start'}");
            $info_collection->insert_one( $first_info );
        };
        if($@){
            log_error( "  **** ERROR: Problem inserting measurement document: $@ " );
            log_error( "  DOCUMENT TO BE INSERTED: ".Dumper($first_info) );
            $ninserts--;
            exit;
        }
    }


} # end loop over unique identfiers

log_info( "NO. OF INDENTIFIERS = ". @identifiers);
log_info( "NO. OF DELETES = $ndeletes");
log_info( "NO. OF INSERTS = $ninserts");

#-----------------------------
sub usage {

    log_info("Usage: $0 [-config <file path>] [-logging <file path>] -database <database> -query <query> [-debug] [-no-change] [-help]");
    exit( 1 );
}

sub _mongo_connect {

    # read config file 
    my $config = GRNOC::Config->new( config_file => $config_file,
                                     force_array => 0 );
    my $mongo_host = $config->get( '/config/mongo/@host' );
    my $mongo_port = $config->get( '/config/mongo/@port' );
    my $rw_user    = $config->get( "/config/mongo/readwrite" );

    my $mongo;
    # try-catch connect to Mongo
    eval {
        $mongo = MongoDB::MongoClient->new(
            host => "$mongo_host:$mongo_port",
            query_timeout => -1,
            username => $rw_user->{'user'},
            password => $rw_user->{'password'}
        );
    };
    if($@){
        log_error("ERROR: Could not connect to Mongo: $@");
    }

    return $mongo;
}
