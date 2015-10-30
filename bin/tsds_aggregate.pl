#!/usr/bin/perl

use strict;
use warnings;

use GRNOC::TSDS::Aggregate;
use JSON::XS;

use Getopt::Long;
use Data::Dumper;

### constants ###

use constant DEFAULT_CONFIG_FILE => '/etc/grnoc/tsds/services/config.xml';
use constant DEFAULT_LOGGING_FILE => '/etc/grnoc/tsds/services/logging.conf';
use constant DEFAULT_LOCK_DIR => '/var/run/grnoc/tsds/services/';
use constant DEFAULT_NUM_PROCESSES => 4;

### command line options ###

my $help;
my $debug;
my $config = DEFAULT_CONFIG_FILE;
my $logging = DEFAULT_LOGGING_FILE;
my $lock_dir = DEFAULT_LOCK_DIR;
my $database;
my $aggregate;
my $query;
my $start;
my $end;
my $num_processes = DEFAULT_NUM_PROCESSES;
my $quiet;
my $pretend;

# parse options from command line
GetOptions( "help|h|?" => \$help,
            "debug" => \$debug,
            "config=s" => \$config,
            "logging=s" => \$logging,
            "lock-dir=s" => \$lock_dir,
            "database=s" => \$database,
            "aggregate=s" => \$aggregate,
            "query=s" => \$query,
            "start=i" => \$start,
            "end=i" => \$end,
            "num-processes=i" => \$num_processes,
            "quiet" => \$quiet,
            "pretend" => \$pretend );

# did they ask for help?
usage() if $help;

# did they specify a query?
if ( defined( $query ) ) {

    # json decode it
    my $json = JSON::XS->new();

    $query = $json->decode( $query );
}

# instantiation retention agent object
my $agent = GRNOC::TSDS::Aggregate->new( config_file => $config,
                                         logging_file => $logging,
                                         lock_dir => $lock_dir,
                                         database => $database,
                                         aggregate => $aggregate,
                                         query => $query,
                                         start => $start,
                                         end => $end,
                                         num_processes => $num_processes,
                                         quiet => $quiet,
                                         pretend => $pretend );

# run with options provided
$agent->aggregate_data();

### private methods ###

sub usage {

    print "Usage: $0 [--config <file path>] [--logging <file path>] [--lock-dir <dir path>] [--database <database>] [--aggregate <name>] [--query <query>] [--start <epoch>] [--end <epoch>] [--num-processes <num>] [--quiet] [--pretend] [--help]\n";
    exit( 1 );
}
