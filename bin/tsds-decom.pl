#!/usr/bin/perl

use strict;
use warnings;

use GRNOC::Log;
use GRNOC::Config;

use MongoDB;
use Parallel::ForkManager;
use Try::Tiny;
use List::MoreUtils qw( natatime );
use Getopt::Long;
use Data::Dumper;

use constant DEFAULT_CONFIG => '/etc/grnoc/tsds-meta-manager/config.xml';
use constant USAGE => "$0: [--config | -c <config file>] [--help | -h]";

my $config_file = DEFAULT_CONFIG;
my $help        = 0;

GetOptions(
    'config|c=s'   => \$config_file,
    'help|h|?'       => \$help,
    ) or die USAGE;

if ($help){
    print USAGE . "\n";
    exit(1);
}

if (! $config_file || ! -e $config_file){
    print "Missing or invalid config file: $config_file\n";
    print USAGE . "\n";
    exit(1);
}

my $config = GRNOC::Config->new( config_file => $config_file,
				 force_array => 0 );

if ( $config->get_error() ) {

    die( $config->get_error()->{'error'} );
}

my $max_procs = $config->get( '/config/max_procs' ) || 1;

# Mongo Parameters
my $mongo_host  = $config->get( '/config/mongo/host' );
my $mongo_port  = $config->get( '/config/mongo/port' );
my $mongo_user  = $config->get( '/config/mongo/username' );
my $mongo_pass  = $config->get( '/config/mongo/password' );

# logging options
my $log_config = $config->get( '/config/logging' );

# init logger
GRNOC::Log->new( config => $log_config );

my %mongo_args = ( host => "$mongo_host:$mongo_port" );

if ( $mongo_user ) {

    $mongo_args{"username"} = $mongo_user;
    $mongo_args{"password"} = $mongo_pass;
}

log_debug( "Parsing <type>s" );

# configured types
$config->{'force_array'} = 1;
my $types = $config->get( '/config/type' );
$config->{'force_array'} = 0;

# no types found?
if ( !defined $types ) {

    die( "no <type> entries found in config $config_file" );
}

# index types by type name
my $types_index = {};

foreach my $type ( @$types ) {

    my $name = $type->{'name'};

    $types_index->{$name} = $type;
}

log_info( "Connecting to mongo w/ $mongo_host:$mongo_port" );

# connect to mongo
my $mongo;

try {

    $mongo = MongoDB::MongoClient->new( %mongo_args );
}

catch {

    log_error( "Unable to connect to mongo: $_" );
    exit( 1 );
};

my @dbs = $mongo->database_names;

foreach my $db_name ( @dbs ) {
    
    # only handle this database if its configured 
    next if ( !$types_index->{$db_name} );

    log_info( "Processing $db_name" );

    process_db( $db_name );
}

# all done
exit( 0 );

sub process_db {

    my $db_name = shift;

    my $db = $mongo->get_database( $db_name );
    my $expire_after = $types_index->{$db_name}{'expire_after'};

    if ( !defined( $expire_after ) ) {

        log_warn( "$db_name has no expire_after set, skipping" );
        return;
    }

    log_debug( "$db_name has expire_after $expire_after" );

    my $measurements = $db->get_collection( 'measurements' );
    
    if (!defined($measurements)) {
        log_warn( "$db_name has no measurements collection, skipping" );
        return;
    }

    my $data = $db->get_collection( 'data' );
    
    if (!defined($data)) {
	log_warn( "$db_name has no data collection, skipping" );
        return;
    }

    my $now = time();
    my $expired_time = $now - $expire_after;

    log_debug( "retrieving all active measurements in database $db_name" );

    my @measurements = $measurements->find( {'end' => undef} )->hint( 'end_1' )->all();

    # no active measurements?
    if ( @measurements == 0 ) {

	log_warn( "$db_name has no active measurements, skipping" );
	return;
    }

    # kind of a dirty hack to make natatime happy
    # if the number of active measurements is < our max procs, reduce
    # max procs to that number
    if ( @measurements < $max_procs ) {

        $max_procs = @measurements;
    }

    my $forker = Parallel::ForkManager->new( $max_procs );

    # divide up all measurements by total number of processes we are told to use
    my $it = natatime( scalar( @measurements ) / $max_procs, @measurements );

    while ( my @block = $it->() ) {

        # fork a child process to handle this set
        my $pid = $forker->start() and next;

	try {

	    # reconnect after fork
	    $mongo = MongoDB::MongoClient->new( %mongo_args );
	    $db = $mongo->get_database( $db_name );
	    $measurements = $db->get_collection( 'measurements' );
	    $data = $db->get_collection( 'data' );	
	    
	    foreach my $doc ( @block ) {
		
		my $identifier = $doc->{'identifier'};
		my $id = $doc->{'_id'};
		
		log_debug( "handling identifier $identifier in database $db_name" );
		log_debug( "finding most recent data doc for identifier $identifier in database $db_name" );

		# need to grab the lock and then fetch the measurement again to make sure it's still
		# decom, something could have come along and updated it since we last fetched it
		# TODO LOCK MEASUREMENT, REFETCH MEASUREMENT AND SKIP UNLESS END STILL = UNDEF
		# MAYBE JUST FETCH UNDEF AGAIN, SHOULD BE SAFE?


		my $recent_doc = $data->find( {'identifier' => $identifier } )
		    ->hint( 'identifier_1_start_1_end_1' )
		    ->fields({'values' => 0}) # project out the values since we don't care about them
		    ->sort( {'_id' => -1} )
		    ->limit( 1 )
		    ->next();
		
		if ( !defined( $recent_doc ) ) {
		    
		    log_debug( "no recent data doc found for identifier $identifier in database $db_name, decomming it" );
		    
		    # If we can't find any docs, we have to just assume $now is the end point
		    decom_doc( doc => $doc,
			       collection => $measurements,
			       end => $now );
		    
		}
		else {
		    # MongoDB stores the document creation time as part of the _id field
		    # which is always there, so we can use this to figure out whether any documents
		    # have been created for this recently. It's a rough approximation of whether
		    # the measurement is still receiving data or not to the document width
		    # granularity. A more precise system may need to be designed at a future
		    # point
		    my $created_at = $recent_doc->{'_id'}->get_time();
		    my $doc_end    = $recent_doc->{'end'};
		    
		    if ( $recent_end < $expired_time ) {
			
			log_debug( "identifier $identifier in database $db_name has updated or end of most recent data doc < expired_time $expired_time, decomming it" );
			
			decom_doc( doc => $doc,
				   collection => $measurements,
				   end => $doc_end );
		    }
		}

		# TODO RELEASE LOCK
		
	    }
	}

	catch {

	    log_error( "Exception occurred in subprocess: $_" );
	};

	$forker->finish();
    }
    
    $forker->wait_all_children();
}

sub decom_doc {

    my ( %args ) = @_;

    my $doc = $args{'doc'};
    my $collection = $args{'collection'};
    my $end = $args{'end'};

    my $doc_start = $doc->{'start'};

    $collection->remove( {'_id' => $doc->{'_id'}} );

    # we can't set the end to before this doc started, this is kind of a weird case
    # but would put us into a strange measurements state so checking for it here.
    # no need to insert the new version, this one was just wrong
    if ($end < $doc_start){
	log_info("Leaving document for identifier = " . $doc->{'identifier'} . " removed because start $doc_start would be greater than proposed end of $end");
	return;
    }

    # we cannot update the 'end' field, so we need to remove and insert with the updated 'end'
    delete $doc->{'_id'};
    $doc->{'end'} = $end;
    $collection->insert( $doc );
}
