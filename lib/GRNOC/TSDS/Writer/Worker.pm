#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::Writer::Worker;

use Moo;
use Types::Standard qw( Str Int HashRef Object Maybe );

use GRNOC::TSDS::Constants;
use GRNOC::TSDS::DataType;
use GRNOC::TSDS::MongoDB;
use GRNOC::TSDS::AggregateDocument;
use GRNOC::TSDS::DataDocument;
use GRNOC::TSDS::Writer::AggregateMessage;
use GRNOC::TSDS::Writer::DataMessage;
use GRNOC::TSDS::RedisLock;

use GRNOC::TSDS::DataService::MetaData;

use MongoDB;
use Net::AMQP::RabbitMQ;
use Cache::Memcached::Fast;
use Tie::IxHash;
use JSON::XS;
use Math::Round qw( nlowmult );
use Time::HiRes qw( time );
use Try::Tiny;

use Data::Dumper;

### constants ###

use constant DATA_CACHE_EXPIRATION => 60 * 60;
use constant AGGREGATE_CACHE_EXPIRATION => 60 * 60 * 48;
use constant MEASUREMENT_CACHE_EXPIRATION => 60 * 60;
use constant QUEUE_PREFETCH_COUNT => 5;
use constant QUEUE_FETCH_TIMEOUT => 10 * 1000;
use constant RECONNECT_TIMEOUT => 10;
use constant PENDING_QUEUE_CHANNEL => 1;
use constant FAILED_QUEUE_CHANNEL => 2;

### required attributes ###

has config => ( is => 'ro',
                required => 1 );

has logger => ( is => 'ro',
                required => 1 );

has queue => ( is => 'ro',
               required => 1 );

### internal attributes ###

has is_running => ( is => 'rwp',
                    default => 0 );

has data_types => ( is => 'rwp',
                    default => sub { {} } );

has mongo_rw => ( is => 'rwp' );

has rabbit => ( is => 'rwp' );

has redislock => ( is => 'rwp' );

has memcache => ( is => 'rwp' );

has locker => ( is => 'rwp' );

has json => ( is => 'rwp' );

has metadata_ds => ( is => 'rwp' );

### public methods ###

sub start {

    my ( $self ) = @_;

    my $queue = $self->queue;

    $self->logger->debug( "Starting." );

    # flag that we're running
    $self->_set_is_running( 1 );

    # change our process name
    $0 = "tsds_writer ($queue) [worker]";

    # setup signal handlers
    $SIG{'TERM'} = sub {

        $self->logger->info( "Received SIG TERM." );
        $self->stop();
    };

    $SIG{'HUP'} = sub {

        $self->logger->info( "Received SIG HUP." );
    };

    # create JSON object
    my $json = JSON::XS->new();

    $self->_set_json( $json );

    my $mongo_conn = new GRNOC::TSDS::MongoDB(config => $self->config);
    if (!defined $mongo_conn) {
        die "Couldn't connect to MongoDB. See logs for more details.";
    }
    $self->_set_mongo_rw($mongo_conn->mongo);

    $self->_redis_connect();

    # connect to memcache
    my $memcache_host = $self->config->memcached_host;
    my $memcache_port = $self->config->memcached_port;

    $self->logger->debug("Connecting to memcached $memcache_host:$memcache_port.");

    my $memcache = Cache::Memcached::Fast->new( {'servers' => [{'address' => "$memcache_host:$memcache_port", 'weight' => 1}]} );
    $self->_set_memcache($memcache);

    # connect to rabbit
    $self->_rabbit_connect();

    # set up metadata_ds object, will handle metadata messages
    my $metadata_ds = GRNOC::TSDS::DataService::MetaData->new(config_file => $self->config->config_file);
    $self->_set_metadata_ds( $metadata_ds );

    $self->logger->debug( 'Starting RabbitMQ consume loop.' );

    # continually consume messages from rabbit queue, making sure we have to acknowledge them
    return $self->_consume_loop();
}

sub stop {

    my ( $self ) = @_;

    $self->logger->debug( 'Stopping.' );

    $self->_set_is_running( 0 );
}

### private methods ###

sub _consume_loop {

    my ( $self ) = @_;

    while ( 1 ) {

        # have we been told to stop?
        if ( !$self->is_running ) {

            $self->logger->debug( 'Exiting consume loop.' );
            return 0;
        }

        # receive the next rabbit message
        my $rabbit_message;

        try {

            $rabbit_message = $self->rabbit->recv( QUEUE_FETCH_TIMEOUT );
        }

        catch {

            $self->logger->error( "Error receiving rabbit message: $_" );

            # reconnect to rabbit since we had a failure
            $self->_rabbit_connect();
        };

        # didn't get a message?
        if ( !$rabbit_message ) {

            $self->logger->debug( 'No message received.' );

            # re-enter loop to retrieve the next message
            next;
        }

        # try to JSON decode the messages
        my $messages;

        try {

            $messages = $self->json->decode( $rabbit_message->{'body'} );
        }

        catch {

            $self->logger->error( "Unable to JSON decode message: $_" );
        };

        if ( !$messages ) {

            try {

                # reject the message and do NOT requeue it since its malformed JSON
                $self->rabbit->reject( PENDING_QUEUE_CHANNEL, $rabbit_message->{'delivery_tag'}, 0 );
            }

            catch {

                $self->logger->error( "Unable to reject rabbit message: $_" );

                # reconnect to rabbit since we had a failure
                $self->_rabbit_connect();
            };
        }

        # retrieve the next message from rabbit if we couldn't decode this one
        next if ( !$messages );

        # make sure its an array (ref) of messages
        if ( ref( $messages ) ne 'ARRAY' ) {

            $self->logger->error( "Message body must be an array." );

            try {

                # reject the message and do NOT requeue since its not properly formed
                $self->rabbit->reject( PENDING_QUEUE_CHANNEL, $rabbit_message->{'delivery_tag'}, 0 );
            }

            catch {

                $self->logger->error( "Unable to reject rabbit message: $_" );

                # reconnect to rabbit since we had a failure
                $self->_rabbit_connect();
            };

            next;
        }

        my $num_messages = @$messages;
        $self->logger->debug( "Processing message containing $num_messages updates." );

        my $t1 = time();

        my $success = $self->_consume_messages( $messages );

        my $t2 = time();
        my $delta = $t2 - $t1;

        $self->logger->debug( "Processed $num_messages updates in $delta seconds." );

        # didn't successfully consume the messages, so reject but requeue the entire message to try again
        if ( !$success ) {

            $self->logger->debug( "Rejecting rabbit message, requeueing." );

            try {
                # push message to failed queue and ack the original message
                $self->rabbit->publish( FAILED_QUEUE_CHANNEL, $self->queue . "_failed", $self->json->encode( \@$messages ), {'exchange' => ''} );
                $self->rabbit->ack( PENDING_QUEUE_CHANNEL, $rabbit_message->{'delivery_tag'} );
            }

            catch {

                $self->logger->error( "Unable to reject rabbit message: $_" );

                # reconnect to rabbit since we had a failure
                $self->_rabbit_connect();
            };
        }

        # successfully consumed message, acknowledge it to rabbit
        else {

            $self->logger->debug( "Acknowledging successful message." );

            try {

                $self->rabbit->ack( PENDING_QUEUE_CHANNEL, $rabbit_message->{'delivery_tag'} );
            }

            catch {

                $self->logger->error( "Unable to acknowledge rabbit message: $_" );

                # reconnect to rabbit since we had a failure
                $self->_rabbit_connect();
            };
        }
    }
}

sub _consume_messages {

    my ( $self, $messages ) = @_;

    # gather all messages to process
    my $data_to_process = [];
    my $aggregates_to_process = [];
    my $meta_to_process = [];

    # keep track and build up all of the bulk operations we'll want to do at the end
    my $bulk_creates = {};
    my $bulk_updates = {};
    my $acquired_locks = [];

    # handle every TSDS message that came within the rabbit message
    foreach my $message ( @$messages ) {

        # make sure message is an object/hash (ref)
        if ( ref( $message ) ne 'HASH' ) {

            $self->logger->error( "Messages must be an object/hash of data, skipping." );
            next;
        }

        my $type = $message->{'type'};
        my $time = $message->{'time'};
        my $interval = $message->{'interval'};
        my $values = $message->{'values'};
        my $meta = $message->{'meta'};
        my $affected = $message->{'affected'};
        my $text = $message->{'text'};
        my $start = $message->{'start'};
        my $end = $message->{'end'};
        my $identifier = $message->{'identifier'};

        # make sure a type was specified
        if ( !defined( $type ) ) {

            $self->logger->error( "No type specified, skipping message." );
            next;
        }

        # does it appear to be an aggregate message?
        if ( $type =~ /^(.+)\.(aggregate|metadata)$/ ) {

            my $data_type_name = $1;
            my $message_type = $2;
            my $data_type = $self->data_types->{$data_type_name};

            # we haven't seen this data type before, re-fetch them
            if ( !$data_type ) {

                my $success = 1;

                # this involves communicating to mongodb which may fail
                try {

                    $self->_fetch_data_types();
                }

                # requeue the message to try again later if mongo communication fails
                catch {

                    $self->logger->error( "Unable to fetch data types from MongoDB: $_" );

                    $success = 0;
                };

                # dont bother handling any more of the messages in this rabbit message
                return 0 if !$success;

                $data_type = $self->data_types->{$data_type_name};
            }

            # detect unknown data type, ignore it
            if ( !$data_type ) {

                $self->logger->warn( "Unknown data type '$data_type_name', skipping." );
                next;
            }

            # was it an aggregate?
            if ( $message_type eq "aggregate" ) {

                my $aggregate_message;

                try {
		    
                    $aggregate_message = GRNOC::TSDS::Writer::AggregateMessage->new( data_type => $data_type,
                                                                                     time => $time,
                                                                                     interval => $interval,
                                                                                     values => $values,
                                                                                     meta => $meta );
                }

                catch {

                    $self->logger->error( $_ );
                };

                # include this to our list of aggregates to process if it was valid
                push( @$aggregates_to_process, $aggregate_message ) if $aggregate_message;
            }
	    elsif ( $message_type eq 'metadata' ) {

		my $meta_update = {
		    "tsds_type" => $data_type_name,
		    "start"     => $time,
		    "end"       => $end
		};
	       
		foreach my $meta_field (keys %$meta){
		    $meta_update->{$meta_field} = $meta->{$meta_field};
		}

		push(@$meta_to_process, $meta_update); 		
	    }
        }

        # must be a data message
        else {

            my $data_type = $self->data_types->{$type};

            # we haven't seen this data type before, re-fetch them
            if ( !$data_type ) {

                my $success = 1;

                # this involves communicating to mongodb, which may fail
                try {

                    $self->_fetch_data_types();
                }

                # requeue the message to try again later if mongo communication fails
                catch {

                    $self->logger->error( "Unable to fetch data types from MongoDB: $_" );

                    $success = 0;
                };

                # dont bother handling any more of the messages in this rabbit message
                return 0 if !$success;

                $data_type = $self->data_types->{$type};
            }

            # detected unknown data type, ignore it
            if ( !$data_type ) {

                $self->logger->warn( "Unknown data type '$type', skipping." );
                next;
            }

            my $data_message;

            try {

                $data_message = GRNOC::TSDS::Writer::DataMessage->new( data_type => $data_type,
                                                                       time => $time,
                                                                       interval => $interval,
                                                                       values => $values,
                                                                       meta => $meta );
            }

            catch {

                $self->logger->error( $_ );

                # release any outstanding locks
                $self->_release_locks( $acquired_locks );
            };

            # include this to our list of data to process if it was valid
            push( @$data_to_process, $data_message ) if $data_message;
        }
    }

    # process all of the data points within this message
    my $success = 1;

    try {

        # at least one aggregate to process
        if ( @$aggregates_to_process > 0 ) {

            $self->logger->debug( "Processing " . @$aggregates_to_process . " aggregate messages." );

            $self->_process_aggregate_messages( messages => $aggregates_to_process,
                                                bulk_creates => $bulk_creates,
                                                bulk_updates => $bulk_updates,
                                                acquired_locks => $acquired_locks );
        }

        # at least one high res data to process
        if ( @$data_to_process > 0 ) {

            $self->logger->debug( "Processing " . @$data_to_process . " data messages." );

            $self->_process_data_messages( messages => $data_to_process,
                                           bulk_creates => $bulk_creates,
                                           bulk_updates => $bulk_updates,
                                           acquired_locks => $acquired_locks );
        }

        # perform all (most, except for data type changes..) create and update operations in bulk
        $self->_process_bulks( $bulk_creates );
        $self->_process_bulks( $bulk_updates );

        # release all the locks we're acquired for the docs we're changing
        $self->_release_locks( $acquired_locks );


	# This does it's own locking, so we'll do that here after we release any locks above.
	if ( @$meta_to_process > 0 ) {
	    $self->metadata_ds()->update_measurement_metadata(values => $meta_to_process, type_field => "tsds_type", fatal => 1);
	}

    }

    catch {

        $self->logger->error( "Error processing messages: $_" );

	$self->_redis_connect();

        # release any outstanding locks
        $self->_release_locks( $acquired_locks );

        $success = 0;
    };

    return $success;
}

sub _release_locks {

    my ( $self, $locks ) = @_;

    foreach my $lock ( @$locks ) {

        $self->redislock->unlock( $lock );
    }
}

sub _process_bulks {

    my ( $self, $bulks ) = @_;

    my @database_names = keys( %$bulks );

    foreach my $database_name ( @database_names ) {

        my @collection_names = keys( %{$bulks->{$database_name}});

        foreach my $collection_name ( @collection_names ) {

            my $bulk = $bulks->{$database_name}{$collection_name};

            $self->logger->debug( "Executing bulk query for $database_name - $collection_name." );

            my $ret = $bulk->execute();

            my $num_errors = $ret->count_writeErrors() + $ret->count_writeConcernErrors();

            # did at least one error occur during the bulk update?
            if ( $num_errors > 0 ) {

                # throw an exception so this entire message will get requeued
                die( "bulk update failed: " . $ret->last_errmsg() );
            }
        }
    }
}

sub _process_data_messages {

    my ( $self, %args ) = @_;

    my $messages = $args{'messages'};
    my $bulk_creates = $args{'bulk_creates'};
    my $bulk_updates = $args{'bulk_updates'};
    my $acquired_locks = $args{'acquired_locks'};

    # all unique value types we're handling per each data type
    my $unique_data_types = {};
    my $unique_value_types = {};

    # all unique measurements we're handling
    my $unique_measurements = {};

    # all unique documents we're handling (and their corresponding data points)
    my $unique_documents = {};

    # handle every message sent, ordered by their timestamp in ascending order
    foreach my $message ( sort { $a->time <=> $b->time } @$messages ) {
        #$self->logger->error("Procesing Message");
        my $data_type = $message->data_type;
        #$self->logger->error("Procesing Message of Type: " . $data_type);
        #$self->logger->error("MetaFields: " . Dumper($message->meta));
        #$self->logger->error("Message Values: " . Dumper($message->values));
        my $measurement_identifier = $message->measurement_identifier;
        #$self->logger->error("Message Identifier: " . $measurement_identifier);
        my $interval = $message->interval;
	my $data_points;

	# this is lazily built so it might fail validation
	try {
	    $data_points = $message->data_points;
	}
	catch {
	    $self->logger->error( "Error building data points for message: $_" );
	};

	next if (! defined $data_points);

        my $time = $message->time;
        my $meta = $message->meta;

        # mark this data type as being found
        $unique_data_types->{$data_type->name} = $data_type;

        # have we handled this measurement already?
        my $unique_measurement = $unique_measurements->{$data_type->name}{$measurement_identifier};

        if ( $unique_measurement ) {

            # keep the older start time, just update its meta data with the latest
            $unique_measurements->{$data_type->name}{$measurement_identifier}{'meta'} = $meta;
        }

        # never seen this measurement before
        else {

            # mark this measurement as being found, and include its meta data and start time
            $unique_measurements->{$data_type->name}{$measurement_identifier} = {'meta' => $meta,
                                                                                 'start' => $time,
                                                                                 'interval' => $interval};
        }

        # determine proper start and end time of document
        my $doc_length = $interval * HIGH_RESOLUTION_DOCUMENT_SIZE;
        my $start = nlowmult( $doc_length, $time );
        my $end = $start + $doc_length;

        # determine the document that this message would belong within
        my $document = GRNOC::TSDS::DataDocument->new( data_type => $data_type,
                                                       measurement_identifier => $measurement_identifier,
                                                       interval => $interval,
                                                       start => $start,
                                                       end => $end );

        # mark the document for this data point if one hasn't been set already
        my $unique_doc = $unique_documents->{$data_type->name}{$measurement_identifier}{$document->start}{$document->end};

        # we've never handled a data point for this document before
        if ( !$unique_doc ) {

            # mark it as being a new unique document we need to handle
            $unique_documents->{$data_type->name}{$measurement_identifier}{$document->start}{$document->end} = $document;
            $unique_doc = $unique_documents->{$data_type->name}{$measurement_identifier}{$document->start}{$document->end};
        }

        # handle every data point that was included in this message
        foreach my $data_point ( @$data_points ) {

            my $value_type = $data_point->value_type;

            # add this as another data point to update/set in the document
            $unique_doc->add_data_point( $data_point );

            # mark this value type as being found
            $unique_value_types->{$data_type->name}{$value_type} = 1;
        }
    }

    # get cache ids for all unique measurements we'll ask about
    my @measurement_cache_ids;

    my @data_types = keys( %$unique_measurements );

    foreach my $data_type ( @data_types ) {

        my @measurement_identifiers = keys( %{$unique_measurements->{$data_type}} );

        foreach my $measurement_identifier ( sort @measurement_identifiers ) {

            my $cache_id = $self->redislock->get_cache_id( type       => $data_type,
							   collection => 'measurements',
							   identifier => $measurement_identifier );

            push( @measurement_cache_ids, $cache_id );
        }
    }

    if ( @measurement_cache_ids ) {

        # grab measurements from our cache
        my $measurement_cache_results = $self->memcache->get_multi( @measurement_cache_ids );

        # potentially create new measurement entries that we've never seen before
        @data_types = keys( %$unique_measurements );

        foreach my $data_type ( sort @data_types ) {

            my @measurement_identifiers = keys( %{$unique_measurements->{$data_type}} );

            foreach my $measurement_identifier ( sort @measurement_identifiers ) {

                my $cache_id = shift( @measurement_cache_ids );

                # this measurement exists in our cache, dont bother creating it
                next if ( $measurement_cache_results->{$cache_id} );

                # potentially create a new entry unless someone else beats us to it
                my $meta = $unique_measurements->{$data_type}{$measurement_identifier}{'meta'};
                my $start = $unique_measurements->{$data_type}{$measurement_identifier}{'start'};
                my $interval = $unique_measurements->{$data_type}{$measurement_identifier}{'interval'};

                $self->_create_measurement_document( identifier => $measurement_identifier,
                                                     data_type => $unique_data_types->{$data_type},
                                                     meta => $meta,
                                                     start => $start,
                                                     interval => $interval,
                                                     bulk_creates => $bulk_creates,
                                                     acquired_locks => $acquired_locks );
            }
        }
    }

    # potentially update the metadata value types for every distinct one found
    @data_types = keys( %$unique_value_types );

    foreach my $data_type ( @data_types ) {

        my @value_types = keys( %{$unique_value_types->{$data_type}} );

        $self->_update_metadata_value_types( data_type => $unique_data_types->{$data_type},
                                             value_types => \@value_types );
    }

    # handle every distinct document that we'll need to update
    @data_types = keys( %$unique_documents );

    foreach my $data_type ( sort @data_types ) {

        my @measurement_identifiers = sort keys( %{$unique_documents->{$data_type}} );

        foreach my $measurement_identifier ( sort @measurement_identifiers ) {

            my @starts = keys( %{$unique_documents->{$data_type}{$measurement_identifier}} );

            foreach my $start ( sort { $a <=> $b } @starts ) {

                my @ends = keys( %{$unique_documents->{$data_type}{$measurement_identifier}{$start}} );

                foreach my $end ( sort { $a <=> $b } @ends ) {

                    my $document = $unique_documents->{$data_type}{$measurement_identifier}{$start}{$end};

                    # process this data document, including all data points contained within it
                    $self->_process_data_document( document => $document,
                                                   bulk_creates => $bulk_creates,
                                                   bulk_updates => $bulk_updates,
                                                   acquired_locks => $acquired_locks );

                    # all done with this document, remove it so we don't hold onto its memory
                    delete( $unique_documents->{$data_type}{$measurement_identifier}{$start}{$end} );
                }
            }
        }
    }
}

sub _process_aggregate_messages {

    my ( $self, %args ) = @_;

    my $messages = $args{'messages'};
    my $bulk_creates = $args{'bulk_creates'};
    my $bulk_updates = $args{'bulk_updates'};
    my $acquired_locks = $args{'acquired_locks'};

    # all unique documents we're handling (and their corresponding data points)
    my $unique_documents = {};

    # handle every message sent, ordered by their timestamp in ascending order
    foreach my $message ( sort { $a->time <=> $b->time } @$messages ) {

        my $data_type = $message->data_type;
        my $measurement_identifier = $message->measurement_identifier;
        my $interval = $message->interval;
        my $time = $message->time;
        my $meta = $message->meta;

        # This is lazily built so it might actually fail type validation
        # when we invoke it for the first time
        my $aggregate_points;

        try {

            $aggregate_points = $message->aggregate_points;
        }
        catch {

            $self->logger->error( "Error processing aggregate update - bad data format: $_" );
        };

        next if (! defined $aggregate_points);

        # determine proper start and end time of document
        my $doc_length = $interval * AGGREGATE_DOCUMENT_SIZE;
        my $start = nlowmult( $doc_length, $time );
        my $end = $start + $doc_length;

        # determine the document that this message would belong within
        my $document = GRNOC::TSDS::AggregateDocument->new( data_type => $data_type,
                                                            measurement_identifier => $measurement_identifier,
                                                            interval => $interval,
                                                            start => $start,
                                                            end => $end );

        # mark the document for this data point if one hasn't been set already
        my $unique_doc = $unique_documents->{$data_type->name}{$measurement_identifier}{$document->start}{$document->end};

        # we've never handled a data point for this document before
        if ( !$unique_doc ) {

            # mark it as being a new unique document we need to handle
            $unique_documents->{$data_type->name}{$measurement_identifier}{$document->start}{$document->end} = $document;
            $unique_doc = $unique_documents->{$data_type->name}{$measurement_identifier}{$document->start}{$document->end};
        }

        # handle every aggregate point that was included in this message
        foreach my $aggregate_point ( @$aggregate_points ) {

            my $value_type = $aggregate_point->value_type;

            # add this as another data point to update/set in the document
            $unique_doc->add_aggregate_point( $aggregate_point );
        }
    }

    # handle every distinct document that we'll need to update
    my @data_types = keys( %$unique_documents );

    foreach my $data_type ( sort @data_types ) {

        my @measurement_identifiers = keys( %{$unique_documents->{$data_type}} );

        foreach my $measurement_identifier ( sort @measurement_identifiers ) {

            my @starts = keys( %{$unique_documents->{$data_type}{$measurement_identifier}} );

            foreach my $start ( sort { $a <=> $b } @starts ) {

                my @ends = keys( %{$unique_documents->{$data_type}{$measurement_identifier}{$start}} );

                foreach my $end ( sort { $a <=> $b } @ends ) {

                    my $document = $unique_documents->{$data_type}{$measurement_identifier}{$start}{$end};

                    # process this aggregate document, including all aggregate points contained within it
                    $self->_process_aggregate_document( document => $document,
                                                        bulk_creates => $bulk_creates,
                                                        bulk_updates => $bulk_updates,
                                                        acquired_locks => $acquired_locks );

                    # all done with this document, remove it so we don't hold onto its memory
                    delete( $unique_documents->{$data_type}{$measurement_identifier}{$start}{$end} );
                }
            }
        }
    }
}

sub _process_data_document {

    my ( $self, %args ) = @_;

    my $document = $args{'document'};
    my $bulk_creates = $args{'bulk_creates'};
    my $bulk_updates = $args{'bulk_updates'};
    my $acquired_locks = $args{'acquired_locks'};

    my $data_type = $document->data_type->name;
    my $measurement_identifier = $document->measurement_identifier;
    my $start = $document->start;
    my $end = $document->end;

    my %new_value_types = %{$document->value_types};

    $self->logger->debug( "Processing data document $data_type / $measurement_identifier / $start / $end." );

    # get lock for this data document
    my $lock = $self->redislock->lock( type       => $data_type,
				       collection => 'data',
				       identifier => $measurement_identifier,
				       start      => $start,
				       end        => $end ) or die "Can't lock data document for $data_type / $measurement_identifier / $start / $end";

    push( @$acquired_locks, $lock );

    my $cache_id = $self->redislock->get_cache_id( type       => $data_type,
						   collection => 'data',
						   identifier => $measurement_identifier,
						   start      => $start,
						   end        => $end );

    # its already in our cache, seen it before
    if ( my $cached = $self->memcache->get( $cache_id ) ) {

        $self->logger->debug( 'Found document in cache, updating.' );

        my $old_value_types = $cached->{'value_types'};

        # update existing document along with its new data points
        ( $document, my $added_value_types ) = $self->_update_data_document( document => $document,
                                                                             old_value_types => $old_value_types,
                                                                             new_value_types => \%new_value_types,
                                                                             bulk_updates => $bulk_updates,
                                                                             acquired_locks => $acquired_locks );

        # will this update add a new value type?
        if ( @$added_value_types > 0 ) {

            # invalidate the cache entry so we fetch it from the db later and verify they were properly added during the bulk op
            $self->memcache->delete( $cache_id );
        }

        # maintain/update existing cache entry
        else {

            $self->memcache->set( $cache_id,
                                  $cached, # This originally set the same value_types as below, but made it so that partial updates didn't work. Keep whatever was already in the cache instead.
                                  DATA_CACHE_EXPIRATION );
        }
    }

    # not in cache, we'll have to query mongo to see if its there
    else {

        $self->logger->debug( 'Document not found in cache.' );

        # retrieve the full updated doc from mongo
        my $live_doc = $document->fetch();

        # document exists in mongo, so we'll need to update it
        if ( $live_doc ) {

            # update our cache with the doc info we found in the db
            $self->memcache->set( $cache_id,
                                  {'value_types' => $live_doc->value_types},
                                  DATA_CACHE_EXPIRATION );

            $self->logger->debug( 'Document exists in mongo, updating.' );

            # update existing document along with its new data points
            ( $document, my $added_value_types ) = $self->_update_data_document( document => $document,
                                                                                 old_value_types => $live_doc->value_types,
                                                                                 new_value_types => \%new_value_types,
                                                                                 bulk_updates => $bulk_updates,
                                                                                 acquired_locks => $acquired_locks );

            # will this update add a new value type?
            if ( @$added_value_types > 0 ) {

                # invalidate the cache entry so we fetch it from the db again later and verify they were properly added during the bulk op
                $self->memcache->delete( $cache_id );
            }
        }

        # doesn't exist in mongo, we'll need to create it along with the data points provided, and
        # make sure there are no overlaps with other docs due to interval change, etc.
        else {

            $self->logger->debug( 'Document does not exist in mongo, creating.' );

            $document = $self->_create_data_document( document => $document,
                                                      bulk_creates => $bulk_creates,
                                                      acquired_locks => $acquired_locks );
        }
    }

    $self->logger->debug( "Finished processing document $data_type / $measurement_identifier / $start / $end." );
}

sub _process_aggregate_document {

    my ( $self, %args ) = @_;

    my $document = $args{'document'};
    my $bulk_creates = $args{'bulk_creates'};
    my $bulk_updates = $args{'bulk_updates'};
    my $acquired_locks = $args{'acquired_locks'};

    my $data_type = $document->data_type;
    my $data_type_name = $data_type->name;
    my $measurement_identifier = $document->measurement_identifier;
    my $start = $document->start;
    my $end = $document->end;
    my $interval = $document->interval;

    my %new_value_types = %{$document->value_types};

    $self->logger->debug( "Processing aggregate document $data_type_name - $interval / $measurement_identifier / $start / $end." );

    # get lock for this aggregate document
    my $lock = $self->redislock->lock( type       => $data_type_name,
                                       collection => "data_$interval",
                                       identifier => $measurement_identifier,
                                       start      => $start,
                                       end        => $end ) or die "Can't lock aggregate data doc for $data_type_name - $interval / $measurement_identifier / $start / $end.";
    push( @$acquired_locks, $lock );

    my $cache_id = $self->redislock->get_cache_id( type       => $data_type_name,
						   collection => "data_$interval",
						   identifier => $measurement_identifier,
						   start      => $start,
						   end        => $end );
    
    # its already in our cache, seen it before
    if ( my $cached = $self->memcache->get( $cache_id ) ) {

        $self->logger->debug( 'Found document in cache, updating.' );

        my $old_value_types = $cached->{'value_types'};

        # update existing document along with its new data points
        ( $document, my $added_value_types )  = $self->_update_aggregate_document( document => $document,
                                                                                   old_value_types => $old_value_types,
                                                                                   new_value_types => \%new_value_types,
                                                                                   bulk_updates => $bulk_updates,
                                                                                   acquired_locks => $acquired_locks );

        # will this update add a new value type?
        if ( @$added_value_types > 0 ) {

            # invalidate the cache entry so we fetch it from the db later and verify they were properly added during the bulk op
            $self->memcache->delete( $cache_id );
        }

        # maintain/update existing cache entry
        else {

            $self->memcache->set( $cache_id,
                                  $cached,
                                  AGGREGATE_CACHE_EXPIRATION );
        }
    }

    # not in cache, we'll have to query mongo to see if its there
    else {

        $self->logger->debug( 'Document not found in cache.' );

        # retrieve the full updated doc from mongo
        my $live_doc = $document->fetch();

        # document exists in mongo, so we'll need to update it
        if ( $live_doc ) {

            # update our cache with the doc info we found in the db
            $self->memcache->set( $cache_id,
                                  {'value_types' => $live_doc->value_types},
                                  AGGREGATE_CACHE_EXPIRATION );

            $self->logger->debug( 'Document exists in mongo, updating.' );

            # update existing document along with its new data points
            ( $document, my $added_value_types ) = $self->_update_aggregate_document( document => $document,
                                                                                      old_value_types => $live_doc->value_types,
                                                                                      new_value_types => \%new_value_types,
                                                                                      bulk_updates => $bulk_updates,
                                                                                      acquired_locks => $acquired_locks );

            # will this update add a new value type?
            if ( @$added_value_types > 0 ) {

                # invalidate the cache entry so we fetch it from the db again later and verify they were properly added during the bulk op
                $self->memcache->delete( $cache_id );
            }
        }

        # doesn't exist in mongo, we'll need to create it along with the aggregate points provided
        else {

            $self->logger->debug( 'Document does not exist in mongo, creating.' );

            my $bulk = $bulk_creates->{$data_type_name}{'data_' . $document->interval};

            # haven't initialized a bulk op for this data type + collection yet
            if ( !defined( $bulk ) ) {

                my $collection = $data_type->database->get_collection( 'data_' . $document->interval );

                $bulk = $collection->initialize_unordered_bulk_op();
                $bulk_creates->{$data_type_name}{'data_' . $document->interval} = $bulk;
            }

            $document = $document->create( bulk => $bulk );
        }
    }

    $self->logger->debug( "Finished processing aggregate document $data_type_name - $interval / $measurement_identifier / $start / $end." );
}


sub _create_data_document {

    my ( $self, %args ) = @_;

    my $document = $args{'document'};
    my $bulk_creates = $args{'bulk_creates'};
    my $acquired_locks = $args{'acquired_locks'};

    # before we insert this new document, we will want to check for existing documents which
    # may have overlapping data with this new one.  this can happen if there was an interval
    # change, since that affects the start .. end range of the document

    my $data_type = $document->data_type;
    my $identifier = $document->measurement_identifier;
    my $start = $document->start;
    my $end = $document->end;
    my $interval = $document->interval;

    $self->logger->debug( "Creating new data document $identifier / $start / $end." );

    # help from http://eli.thegreenplace.net/2008/08/15/intersection-of-1d-segments
    my $query = Tie::IxHash->new( 'identifier' => $identifier,
                                  'start' => {'$lt' => $end},
                                  'end' => {'$gt' => $start} );

    # get this document's data collection
    my $data_collection = $data_type->database->get_collection( 'data' );

    $self->logger->debug( 'Finding existing overlapping data documents before creation.' );

    # the ids of the overlaps we found
    my @overlap_ids;

    # the cache ids of the overlaps we found
    my @overlap_cache_ids;

    # unique documents that the data points, after altering their interval, will belong in
    my $unique_documents = {};

    # add this new document as one of the unique documents that will need to get created
    $unique_documents->{$identifier}{$start}{$end} = $document;

    # specify index hint to address occasional performance problems executing this query
    my $overlaps = $data_collection->find( $query )->hint( 'identifier_1_start_1_end_1' )->fields( {'interval' => 1,
                                                                                                    'start' => 1,
                                                                                                    'end' => 1} );

    # handle every existing overlapping doc, if any
    while ( my $overlap = $overlaps->next ) {

        my $id = $overlap->{'_id'};
        my $overlap_interval = $overlap->{'interval'};
        my $overlap_start = $overlap->{'start'};
        my $overlap_end = $overlap->{'end'};

        # keep this as one of the docs that will need removed later
        push( @overlap_ids, $id );

        # determine cache id for this doc
        my $cache_id = $self->redislock->get_cache_id( type       => $data_type->name,
						       collection => 'data',
						       identifier => $identifier,
						       start      => $overlap_start,
						       end        => $overlap_end );

        push( @overlap_cache_ids, $cache_id );

        # grab lock for this doc
        my $lock = $self->redislock->lock( type       => $data_type->name,
                                           collection => 'data',
                                           identifier => $identifier,
                                           start      => $overlap_start,
                                           end        => $overlap_end ) or die "Can't lock overlapping data doc for $identifier";

        push( @$acquired_locks, $lock );

        $self->logger->debug( "Found overlapping data document with interval: $overlap_interval start: $overlap_start end: $overlap_end." );

        # create object representation of this duplicate doc
        my $overlap_doc = GRNOC::TSDS::DataDocument->new( data_type => $data_type,
                                                          measurement_identifier => $identifier,
                                                          interval => $overlap_interval,
                                                          start => $overlap_start,
                                                          end => $overlap_end );

        # fetch entire doc to grab its data points
        $overlap_doc->fetch( data => 1 );

        # handle every data point in this overlapping doc
        my $data_points = $overlap_doc->data_points;

        foreach my $data_point ( @$data_points ) {

            # set the *new* interval we'll be using for this data point
            $data_point->interval( $interval );

            # determine proper start and end time of *new* document
            my $doc_length = $interval * HIGH_RESOLUTION_DOCUMENT_SIZE;
            my $new_start = int($data_point->time / $doc_length) * $doc_length;
            my $new_end = $new_start + $doc_length;

            # mark the document for this data point if one hasn't been set already
            my $unique_doc = $unique_documents->{$identifier}{$new_start}{$new_end};

            # we've never handled a data point for this document before
            if ( !$unique_doc ) {

		# determine the *new* document that this message would belong within
		my $new_document = GRNOC::TSDS::DataDocument->new( data_type => $data_type,
								   measurement_identifier => $identifier,
								   interval => $interval,
								   start => $new_start,
								   end => $new_end );	       

                # mark it as being a new unique document we need to handle
                $unique_documents->{$identifier}{$new_start}{$new_end} = $new_document;
                $unique_doc = $unique_documents->{$identifier}{$new_start}{$new_end};
            }

            # add this as another data point to update/set in the document, if needed
            $unique_doc->add_data_point( $data_point ) if ( defined $data_point->value );
        }
    }

    # process all new documents that get created as a result of splitting the old document up
    my @measurement_identifiers = keys( %$unique_documents );

    foreach my $measurement_identifier ( @measurement_identifiers ) {

        my @starts = keys( %{$unique_documents->{$measurement_identifier}} );

        foreach my $start ( @starts ) {

            my @ends = keys( %{$unique_documents->{$measurement_identifier}{$start}} );

            foreach my $end ( @ends ) {

                my $unique_document = $unique_documents->{$measurement_identifier}{$start}{$end};

                my $bulk = $bulk_creates->{$data_type->name}{'data'};

                # haven't initialized a bulk op for this data type + collection yet
                if ( !defined( $bulk ) ) {

                    $bulk = $data_collection->initialize_unordered_bulk_op();
                    $bulk_creates->{$data_type->name}{'data'} = $bulk;
                }

                $self->logger->debug( "Creating new data document $measurement_identifier / $start / $end." );
                $unique_document->create( bulk => $bulk );
            }
        }
    }

    # remove all old documents that are getting replaced with new docs
    if ( @overlap_ids > 0 ) {

        # first remove from mongo
        $data_collection->delete_many( {'_id' => {'$in' => \@overlap_ids}} );

        # also must remove them from our cache since they should no longer exist
        $self->memcache->delete_multi( @overlap_cache_ids );
    }

    return $document;
}

sub _update_data_document {

    my ( $self, %args ) = @_;

    my $document = $args{'document'};
    my $old_value_types = $args{'old_value_types'};
    my $new_value_types = $args{'new_value_types'};
    my $bulk_updates = $args{'bulk_updates'};
    my $acquired_locks = $args{'acquired_locks'};

    # do we need to add any value types to the document?
    my @value_types_to_add;

    my @new_value_types = keys( %$new_value_types );
    my @old_value_types = keys( %$old_value_types );

    foreach my $new_value_type ( @new_value_types ) {

        # already in the doc
        next if ( $old_value_types->{$new_value_type} );

        # must be new
        push( @value_types_to_add, $new_value_type );
    }

    # did we find at least one new value type not in the doc?
    if ( @value_types_to_add ) {

        $self->logger->debug( "Adding new value types " . join( ',', @value_types_to_add ) . " to document." );

        $document->add_value_types( \@value_types_to_add );
    }

    my $data_type = $document->data_type;
    my $collection_name = 'data';

    my $bulk = $bulk_updates->{$data_type->name}{$collection_name};

    # haven't initialized a bulk op for this data type + collection yet
    if ( !defined( $bulk ) ) {

        my $collection = $data_type->database->get_collection( $collection_name );

        $bulk = $collection->initialize_unordered_bulk_op();
        $bulk_updates->{$data_type->name}{$collection_name} = $bulk;
    }

    $document->update( bulk => $bulk );

    return ( $document, \@value_types_to_add );
}

sub _update_aggregate_document {

    my ( $self, %args ) = @_;

    my $document = $args{'document'};
    my $old_value_types = $args{'old_value_types'};
    my $bulk_updates = $args{'bulk_updates'};
    my $acquired_locks =$args{'acquired_locks'};

    # do we need to add any value types to the document?
    my @value_types_to_add;

    foreach my $new_value_type ( keys %{$document->value_types} ) {

        # already in the doc
        next if ( $old_value_types->{$new_value_type} );

        # must be new
        push( @value_types_to_add, $new_value_type );
    }

    # did we find at least one new value type not in the doc?
    if ( @value_types_to_add ) {

        $self->logger->debug( "Adding new value types " . join( ',', @value_types_to_add ) . " to document." );

        $document->add_value_types( \@value_types_to_add );
    }

    my $data_type = $document->data_type;
    my $collection_name = 'data_' . $document->interval;

    my $bulk = $bulk_updates->{$data_type->name}{$collection_name};

    # haven't initialized a bulk op for this data type + collection yet
    if ( !defined( $bulk ) ) {

        my $collection = $data_type->database->get_collection( $collection_name );

        $bulk = $collection->initialize_unordered_bulk_op();
        $bulk_updates->{$data_type->name}{$collection_name} = $bulk;
    }

    $document->update( bulk => $bulk );

    return ( $document, \@value_types_to_add );
}

sub _update_metadata_value_types {

    my ( $self, %args ) = @_;

    my $data_type = $args{'data_type'};
    my $new_value_types = $args{'value_types'};

    # determine all the cache ids for all these metadata value types
    my @cache_ids;

    foreach my $new_value_type ( @$new_value_types ) {

        # include this value type in its data type entry
        $self->data_types->{$data_type->name}->value_types->{$new_value_type} = {'description' => $new_value_type,
                                                                                 'units' => $new_value_type};

        my $cache_id = $self->redislock->get_cache_id( type => $data_type->name,
						       collection => 'metadata',
						       identifier => $new_value_type );

        push( @cache_ids, $cache_id );
    }

    # consult our cache to see if any of them dont exists
    my $cache_results = $self->memcache->get_multi( @cache_ids );

    my $found_missing = 0;

    foreach my $cache_id ( @cache_ids ) {

        # cache hit
        next if ( $cache_results->{$cache_id} );

        # found a value type we've never seen before
        $found_missing = 1;
        last;
    }

    # no new value types found to update
    return if ( !$found_missing );

    # get metadata collection for this data type
    my $metadata_collection = $data_type->database->get_collection( 'metadata' );

    # get lock for this metadata document
    my $lock = $self->redislock->lock( type => $data_type->name,
				       collection => 'metadata' ) or die "Can't lock metadata for " . $data_type->name;

    # grab the current metadata document
    my $doc = $metadata_collection->find_one( {}, {'values' => 1} );

    # error if there is none present
    if ( !$doc ) {

        $self->redislock->unlock( $lock );

        die( 'No metadata document found for database ' . $data_type->name . '.' );
    }

    my $updates = {};

    # find any new value types
    foreach my $new_value_type ( @$new_value_types ) {

        # skip it if it already exists
        next if ( exists( $doc->{'values'}{$new_value_type} ) );

        $self->logger->debug( "Adding new value type $new_value_type to database " . $data_type->name . "." );

        # found a new one that needs to be added
        $updates->{"values.$new_value_type"} = {'description' => $new_value_type,
                                                'units' => $new_value_type};
    }

    # is there at least one update to perform?
    if ( keys( %$updates ) > 0 ) {

        # update the single metadata document with all new value types found
        $metadata_collection->update_one( {},
                                          {'$set' => $updates} );
    }

    # mark all value types in our cache
    my @multi = map { [$_ => 1] } @cache_ids;
    $self->memcache->set_multi( @multi );

    # all done, release our lock on this metadata document
    $self->redislock->unlock( $lock );
}

sub _create_measurement_document {

    my ( $self, %args ) = @_;

    my $identifier = $args{'identifier'};
    my $data_type = $args{'data_type'};
    my $meta = $args{'meta'};
    my $start = $args{'start'};
    my $interval = $args{'interval'};
    my $bulk_creates = $args{'bulk_creates'};
    my $acquired_locks = $args{'acquired_locks'};

    $self->logger->debug( "Measurement $identifier in database " . $data_type->name . " not found in cache." );

    # get lock for this measurement identifier
    my $lock = $self->redislock->lock( type       => $data_type->name,
				       collection => 'measurements',
				       identifier => $identifier ) or die "Can't lock measurements for $identifier";
    
    push( @$acquired_locks, $lock );

    # get measurement collection for this data type
    my $measurement_collection = $data_type->database->get_collection( 'measurements' );

    # see if it exists in the database (and is active)
    my $query = Tie::IxHash->new( identifier => $identifier,
                                  end => undef );

    my $exists = $measurement_collection->count( $query );

    # doesn't exist yet
    if ( !$exists ) {

        $self->logger->debug( "Active measurement $identifier not found in database " . $data_type->name . ", adding." );

        my $metadata_fields = $data_type->metadata_fields;

        my $fields = Tie::IxHash->new( identifier => $identifier,
                                       start => $start + 0,
                                       end => undef,
                                       last_updated => $start + 0 );

        while ( my ( $field, $value ) = each( %$meta ) ) {

            # skip it if its not a required meta field for this data type, the writer should only ever set those
            next if ( !$metadata_fields->{$field}{'required'} );

            $fields->Push( $field => $value );
        }

        # create it
        $measurement_collection->insert_one( $fields );
    }

    # mark it in our known cache so no one ever tries to add it again
    my $cache_id = $self->redislock->get_cache_id( type       => $data_type->name,
						   collection => 'measurements',
						   identifier => $identifier );

    my $cache_duration = MEASUREMENT_CACHE_EXPIRATION;

    # use longer cache duration for measurements not submitted often
    $cache_duration = $interval * 2 if ( $interval * 2 > $cache_duration );

    my $cached_result = $self->memcache->set( $cache_id, 1, $cache_duration );

    if (! $cached_result ) {
	$self->logger->warn( "Unable to set cache entry for $cache_id" );
    }
}

sub _fetch_data_types {

    my ( $self ) = @_;

    $self->logger->debug( 'Getting data types.' );

    my $data_types = {};

    # determine databases to ignore
    my $ignore_databases = {};
    foreach my $database (@{$self->config->mongodb_ignore_databases}) {
        $self->logger->debug( "Ignoring database '$database'." );
        $ignore_databases->{$database} = 1;
    }

    # grab all database names in mongo
    my @database_names = $self->mongo_rw->database_names();

    foreach my $database ( @database_names ) {

        # skip it if its marked to be ignored
        next if ( $ignore_databases->{$database} || $database =~ /^_/ );

        $self->logger->debug( "Storing data type for database $database." );

        my $data_type;

        try {

            $data_type = GRNOC::TSDS::DataType->new( name => $database,
                                                     database => $self->mongo_rw->get_database( $database ) );
        }
        catch {
            $self->logger->warn( $_ );
        };

        next if !$data_type;

        # store this as one of our known data types
        $data_types->{$database} = $data_type;
    }

    # update the list of known data types
    $self->_set_data_types( $data_types );
}

sub _redis_connect {
    my ( $self ) = @_;

    while ( 1 ) {

	my $connected = 0;

	try {
	    $self->_set_redislock(GRNOC::TSDS::RedisLock->new(config => $self->config));
	    $connected = 1;
	}
	catch {
	    $self->logger->error( "Error connecting to Redis: $_" );
	};

	last if $connected;
	sleep( RECONNECT_TIMEOUT );
    }
}

sub _rabbit_connect {

    my ( $self ) = @_;

    my $rabbit_host = $self->config->rabbitmq_host;
    my $rabbit_port = $self->config->rabbitmq_port;
    my $rabbit_queue = $self->queue;

    while ( 1 ) {

        $self->logger->info( "Connecting to RabbitMQ $rabbit_host:$rabbit_port." );

        my $connected = 0;

        try {
            my $rabbit = Net::AMQP::RabbitMQ->new();

            $rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );

            # open channel & declare queue for pending writes
            $rabbit->channel_open( PENDING_QUEUE_CHANNEL );
            $rabbit->queue_declare( PENDING_QUEUE_CHANNEL, $rabbit_queue, {'auto_delete' => 0} );
            $rabbit->basic_qos( PENDING_QUEUE_CHANNEL, { prefetch_count => QUEUE_PREFETCH_COUNT } );

            # open channel & declare queue for failed writes
            $rabbit->channel_open( FAILED_QUEUE_CHANNEL );
            $rabbit->queue_declare( FAILED_QUEUE_CHANNEL, $self->queue . "_failed", {'auto_delete' => 0} );

            # start consuming messages
            $rabbit->consume( PENDING_QUEUE_CHANNEL, $rabbit_queue, {'no_ack' => 0} );

            $self->_set_rabbit( $rabbit );

            $connected = 1;
        }

        catch {

            $self->logger->error( "Error connecting to RabbitMQ: $_" );
        };

        last if $connected;

        $self->logger->info( "Reconnecting after " . RECONNECT_TIMEOUT . " seconds..." );
        sleep( RECONNECT_TIMEOUT );
    }
}

1;
