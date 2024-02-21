#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::MeasurementDecommer;

use Moo;
use Types::Standard qw( Int );

use GRNOC::Config;
use GRNOC::CLI;
use GRNOC::Log;

use GRNOC::TSDS::RedisLock;
use GRNOC::TSDS::Constants;

use MongoDB;
use Parallel::ForkManager;
use List::MoreUtils qw( natatime );
use Try::Tiny;

use Data::Dumper;

### required attributes ###

has config_file => ( is => 'ro',
                     required => 1 );

### attributes ###

has database => ( is => 'rw' );
		  

### internal attributes ###

has config => ( is => 'rwp' );

has now   => ( is => 'rwp' );

has mongo => ( is => 'rwp' );

has max_procs => ( is => 'rwp',
		   isa => Int );

### constructor builder ###

sub BUILD {

    my ( $self ) = @_;

    # create and store config object
    my $config = GRNOC::Config->new( config_file => $self->config_file,
                                     force_array => 0 );

    $self->_set_config( $config );

    # connect to mongo
    $self->_mongo_connect();

    # figure out max procs
    my $max_procs = $self->config()->get('/config/decom/@max_procs');
    $self->_set_max_procs($max_procs);

    # determine current timestamp
    $self->_set_now( time() );

    return $self;
}

### public methods ###

sub decom_metadata {
    my ( $self ) = @_;
 
    my @dbs = $self->mongo()->database_names;

    my $expired = {};

    foreach my $db_name (@dbs){

	# skip known non tsds ones
	next if ($db_name eq 'admin' || $db_name eq 'config');

	# skip if we have a specified one and this isn't it
	next if ($self->database() && $db_name ne $self->database());

	my $db = $self->mongo()->get_database($db_name);

	my $metadata;
	try {
	    $metadata = $db->get_collection('metadata')->find_one();
	}
	catch {
	    log_error("Unable to query metadata for $db_name, skipping: $_");
	};

	if (! $metadata){
	    log_debug("Skipping $db_name due to no metadata");
	    next;
	}

	my $expire_after = $metadata->{'expire_after'};

	if (! $expire_after || $expire_after !~ /^\d+$/){
	    log_info("Skipping $db_name due to no or malformed expire_after in metadata");
	    next;
	}

	my $num_expired = $self->_process_db($db_name, $db, $expire_after);

	$expired->{$db_name} = $num_expired;
    }

    return $expired;
}

sub _process_db {
    my ( $self, $db_name, $db, $expire_after ) = @_;

    log_debug("processing database $db_name with expire $expire_after");
    
    my $data         = $db->get_collection( 'data' );
    my $measurements = $db->get_collection( 'measurements' );
    
    my $now = $self->now();
    my $expired_time = $now - $expire_after;

    log_debug( "retrieving all active measurements in database $db_name" );

    my @measurements = $measurements->find( {'end' => undef} )->hint( 'end_1' )->all();

    log_info("found " . scalar(@measurements) . " measurements");

    # no active measurements?
    if ( @measurements == 0 ) {
	log_info( "$db_name has no active measurements, skipping" );
	return 0;
    }

    my $max_procs = $self->max_procs();

    # kind of a dirty hack to make natatime happy
    # if the number of active measurements is < our max procs, reduce
    # max procs to that number
    if ( @measurements < $max_procs ) {
        $max_procs = @measurements;
    }

    my $forker = Parallel::ForkManager->new( $max_procs );

    # keep track of how many total things we're decomming
    my $total_decommed = 0;
    $forker->run_on_finish( sub {
	my ($pid, $exit_code, $ident, $exit, $dump, $data) = @_;
	if (ref $data){
	    $total_decommed += $data->[0];
	}
    });
    
    # divide up all measurements by total number of processes we are told to use
    my $it = natatime( scalar( @measurements ) / $max_procs, @measurements ); 

    while ( my @block = $it->() ) {

        # fork a child process to handle this set
        my $pid = $forker->start() and next;

	my $num_decommed = 0;

	try {

	    # reconnect after fork
	    my $mongo = $self->_mongo_connect();
	    $db = $mongo->get_database( $db_name );
	    $measurements = $db->get_collection( 'measurements' );
	    $data = $db->get_collection( 'data' );	

	    my $redislock = GRNOC::TSDS::RedisLock->new(config => $self->config());

	    foreach my $doc ( @block ) {
		
		my $identifier = $doc->{'identifier'};
		
		log_debug( "handling identifier $identifier in database $db_name" );
		log_debug( "finding most recent data doc for identifier $identifier in database $db_name" );

		# need to grab the lock and then fetch the measurement again to make sure it's still
		# decom, something could have come along and updated it since we last fetched it
		my $lock = $redislock->lock(type       => $db_name,
					    collection => "measurements",
					    identifier => $identifier);
		if (! $lock){
		    log_warn("Unable to lock $identifier in $db_name, skipping");
		    next;
		}

		# now that it's locked re-fetch the undef doc again in case something 
		# else had touched it in between
		$doc = $measurements->find_one({'end' => undef, 'identifier' => $identifier});

		if (! $doc){
		    log_warn("Doc for $identifier stopped being undef between first and second fetch, skipping");
		    $redislock->unlock($lock);
		    next;
		}


		my $id = $doc->{'_id'};

		my $recent_doc = $data->find( {'identifier' => $identifier } )
		    ->hint( 'identifier_1_start_1_end_1' )
		    ->fields({'start' => 1, 'end' => 1, '_id' => 0}) # project in only the values we need
		    ->sort( {'start' => -1} )
		    ->limit( 1 )
		    ->next();


		# if we can't find the most recent doc by opportunistically looking at the last set of highrest
		# data docs, we need to do a slightly deeper scan to find the last "end" date of the data
		# This should be very uncommon
		if ( !defined( $recent_doc ) ) {
		    $num_decommed++;


		    log_debug( "no recent data doc found for identifier $identifier in database $db_name, decomming it" );
		    
		    # If we can't find any docs, we have to just assume $now is the end point
		    $self->_decom_doc( doc        => $doc,
				       collection => $measurements,
				       end        => $now );
		    
		}
		else {
		    # MongoDB stores the document creation time as part of the _id field
		    # which is always there, so we can use this to figure out whether any documents
		    # have been created for this recently. It's a rough approximation of whether
		    # the measurement is still receiving data or not to the document width
		    # granularity. A more precise system may need to be designed at a future
		    # point
		    my $created_at = $recent_doc->{'start'};
		    my $doc_end    = $recent_doc->{'end'};

		    log_debug("$db_name / $identifier most recent created at = $created_at");
		    
		    # safety check, I don't think this be possible to hit but just in case since
		    # the consequences would be bad
		    if (! $created_at){
		    	log_error("Unable to determine time from _id field for $identifier in $db_name");
		    }
		    elsif ( $created_at < $expired_time ) {			
		    	$num_decommed++;			
			
		    	log_debug( "identifier $identifier in database $db_name most recent document was created at $created_at which is earlier than the last expiration time at $expired_time" );		
			
		    	$self->_decom_doc( doc        => $doc,
		    			   collection => $measurements,
		    			   end        => $doc_end );
		    }
		}
		
		# release our lock now that we're done
		$redislock->unlock($lock);
	    }
	}

	catch {

	    log_error( "Exception occurred in subprocess: $_" );
	};

	$forker->finish(0, [$num_decommed]);
    }
    
    $forker->wait_all_children();

    return $total_decommed;
}

sub _decom_doc {
    my ( $self, %args) = @_;

    my $doc = $args{'doc'};
    my $collection = $args{'collection'};
    my $end = $args{'end'};

    if (! defined $end){
	log_warn("Missing end time to _decom_doc, bailing on request");
	return;
    }

    my $doc_start = $doc->{'start'};

    $collection->delete_one( {'_id' => $doc->{'_id'}} );

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
    $collection->insert_one( $doc );
}


sub _mongo_connect {

    my ( $self ) = @_;

    my $mongo_host = $self->config->get( '/config/mongo/@host' );
    my $mongo_port = $self->config->get( '/config/mongo/@port' );
    my $rw_user    = $self->config->get( "/config/mongo/readwrite" );

    log_debug( "Connecting to MongoDB $mongo_host:$mongo_port." );
    log_debug( "Connecting to MongoDB as readwrite on $mongo_host:$mongo_port." );

    my $mongo;
    eval {
        $mongo = MongoDB::MongoClient->new(
            host => "$mongo_host:$mongo_port",     
            username => $rw_user->{'user'},
            password => $rw_user->{'password'},
	    socket_timeout_ms => 120 * 1000,
	    max_time_ms => 60 * 1000,
        );

    };
    if($@){
        die "Could not connect to Mongo: $@";
    }

    $self->_set_mongo( $mongo );
}

1;
