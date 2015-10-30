package GRNOC::TSDS::Aggregate;

use Moo;

use GRNOC::TSDS::Aggregate::Histogram;
use GRNOC::TSDS::Constants;

use GRNOC::Config;
use GRNOC::CLI;
use GRNOC::Log;

use boolean;
use MongoDB;
use Tie::IxHash;
use POSIX qw( ceil );
use Math::Round qw( nlowmult nhimult );
use JSON::XS;
use Parallel::ForkManager;
use List::MoreUtils qw( natatime );
use LockFile::Simple;

use Data::Dumper;

### required attributes ###

has config_file => ( is => 'ro',
                     required => 1 );

has logging_file => ( is => 'ro',
                      required => 1 );

has lock_dir => ( is => 'ro',
                  required => 1 );

### optional attributes ###

has start => ( is => 'ro',
               required => 0,
               coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

has end => ( is => 'ro',
             required => 0,
             coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

has database => ( is => 'ro',
                  required => 0 );

has aggregate => ( is => 'rw',
                   required => 0 );

has query => ( is => 'ro',
               required => 0 );

has num_processes => ( is => 'ro',
                       default => 1 );

has quiet => ( is => 'ro',
               default => 0 );

has pretend => ( is => 'ro',
                 default => 0 );

### internal attributes ###

has config => ( is => 'rwp' );

has logger => ( is => 'rwp' );

has mongo_rw => ( is => 'rwp' );

has json => ( is => 'rwp' );

has is_automatic_run => ( is => 'rwp' );

has locker => ( is => 'rwp' );

### constructor builder ###

sub BUILD {

    my ( $self ) = @_;

    # create and store logger object
    my $grnoc_log = GRNOC::Log->new( config => $self->logging_file );
    my $logger = GRNOC::Log->get_logger();

    $self->_set_logger( $logger );

    # create and store config object
    my $config = GRNOC::Config->new( config_file => $self->config_file,
                                     force_array => 0 );

    $self->_set_config( $config );

    # create and store json object
    my $json = JSON::XS->new();

    $self->_set_json( $json );

    # create and store lock file object
    my $locker = LockFile::Simple->make( -max => 1 );

    $self->_set_locker( $locker );

    # connect to mongo
    $self->_mongo_connect();

    # mark whether this run is an automatic run (as opposed to custom run)
    $self->_set_is_automatic_run( !defined $self->start && !defined $self->end && !defined $self->query );

    return $self;
}

### public methods ###

sub aggregate_data {

    my ( $self ) = @_;

    # determine all possible databases we need to handle
    my @database_names = $self->mongo_rw()->database_names();

    # handle each database
    foreach my $database_name ( @database_names ) {

        # skip it if its a 'private' database prefixed with _ or one of the listed databases to ignore
        if ( $database_name =~ /^_/ || IGNORE_DATABASES->{$database_name} ) {

            $self->logger->debug( "Ignoring database $database_name." );
            next;
        }

        # skip it if we specified a single database and its not this one
        next if ( defined( $self->database ) && $database_name ne $self->database );

        # get ahold of this mongo database instance
        my $database = $self->mongo_rw()->get_database( $database_name );

        # aggregate all necessary data for this database
        $self->_aggregate_database( $database );
    }

    return 1;
}

### private methods ###

sub _aggregate_database {

    my ( $self, $database ) = @_;

    # get the aggregate and measurement collections
    my $aggregate_collection = $database->get_collection( 'aggregate' );
    my $measurement_collection = $database->get_collection( 'measurements' );

    # get all configured aggregates for this database, sorted by interval + eval position
    my $sort = Tie::IxHash->new( interval => 1,
                                 eval_position => 1 );

    my @aggregates = $aggregate_collection->find( {} )->sort( $sort )->all();

    # map aggregate name to aggregate
    my $aggregate_index = {};

    # interval + aggregate + measurement
    my $aggregate_measurement_mappings = {};

    # only handle the most recent measurement entry at each interval for each identifier
    my $found_identifiers = {};

    foreach my $aggregate ( @aggregates ) {

        my $name = $aggregate->{'name'};
        my $interval = $aggregate->{'interval'};
        my $meta = $aggregate->{'meta'};
        my $user_query = $self->query;


        # skip it if they specified a single aggregate and its not this one
        next if ( defined( $self->aggregate ) && $name ne $self->aggregate );

        my $query = Tie::IxHash->new();

        if ( $meta ) {
            $meta = $self->json->decode( $meta );

            while ( my ( $key, $value ) = each( %$meta ) ) {

                $query->Push( $key => $value );
            }
        }

        if ( $user_query ) {

            while ( my ( $key, $value ) = each( %$user_query ) ) {

                $query->Push( $key => $value );
            }
        }

        $aggregate_index->{$name} = $aggregate;

        # first time we've handled this interval
        $aggregate_measurement_mappings->{$interval} = {} if ( !$aggregate_measurement_mappings->{$interval} );

        # first time we've handled this interval + aggregate
        $aggregate_measurement_mappings->{$interval}{$name} = {} if ( !$aggregate_measurement_mappings->{$interval}{$name} );

        # find the measurements which match this aggregate entry
        my $measurements = $measurement_collection->find( $query );

        while ( my $measurement = $measurements->next() ) {
            my $identifier = $measurement->{'identifier'};
            my $start = $measurement->{'start'};

            my $prior_start = $found_identifiers->{$interval}{$identifier};

            # we've previously handled this identifier before for an aggregate at this interval
            if ( defined $prior_start ) {

                # is this measurement earlier than the one we've already seen for it?
                if ( $start < $prior_start ) {

                    # skip this measurement in this aggregate as there is a later measurement entry for it
                    next;
                }
            }

            # mark this measurement identifier as being handled at this interval + time
            $found_identifiers->{$interval}{$identifier} = $start;

            # remove this measurement as being part of any other aggregates at this interval
            my @others = keys( %{$aggregate_measurement_mappings->{$interval}} );

            foreach my $other ( @others ) {

                delete( $aggregate_measurement_mappings->{$interval}{$other}{$identifier} );
            }

            # mark this aggregate for this measurement at this interval to be used
            $aggregate_measurement_mappings->{$interval}{$name}{$identifier} = $measurement;
        }
    }

    my @intervals = keys( %$aggregate_measurement_mappings );

    foreach my $interval ( @intervals ) {

        my @aggregates = keys( %{$aggregate_measurement_mappings->{$interval}} );

        foreach my $aggregate ( @aggregates ) {

            my $measurements = $aggregate_measurement_mappings->{$interval}{$aggregate};

            $self->_aggregate( database => $database,
                               aggregate => $aggregate_index->{$aggregate},
                               measurements => $measurements );
        }
    }
}

sub _aggregate {

    my ( $self, %args ) = @_;

    my $database = $args{'database'};
    my $aggregate = $args{'aggregate'};
    my $measurements = $args{'measurements'};

    my $lock;

    if ( $self->is_automatic_run ) {

        # create lock for this database + aggregate
        $lock = $database->{'name'} . "_" . $aggregate->{'name'} . "_aggregate" . ".lock";
        my $locked = $self->locker->lock( $lock, $self->lock_dir . $lock );

        # unable to grab lock
        if ( !$locked ) {

            $self->logger->warn( "Unable to get lock file for aggregate $aggregate->{'name'} in database $database->{'name'}." );
            return;
        }
    }

    # get the aggregate attributes
    my $aggregate_seconds = $aggregate->{'interval'};
    my $aggregate_last_run = $aggregate->{'last_run'};

    # there is no aggregation duration specified for this aggregate entry (default hires?)
    if ( !defined( $aggregate_seconds ) ) {

        $self->logger->warn( "No interval specified for aggregate $aggregate->{'name'} in database $database->{'name'}." );
        return;
    }

    $self->logger->info( "Aggregating data for database $database->{'name'} using aggregate $aggregate->{'name'}." );

    my $now = time();

    my $start;
    my $end;

    # was this not an 'automatic' run but a custom one?
    if ( !$self->is_automatic_run ) {

        $self->logger->debug( "Custom run, using start and end times specified (if any)." );

        $start = $self->start || 0;
        $end = $self->end || $now;
    }

    # automatic run, try to use the last_run set for this aggregate
    else {

        $self->logger->debug( "Automatic run, aggregating from last run until now." );

        $start = $aggregate_last_run;
        $end = $now;
    }

    # dont try to aggregrate from now until epoch 0! find the minimum possible timestamp of data instead and start from it
    if ( !$start ) {

        $self->logger->debug( "No start time found, determining the minimum possible timestamp from all data." );

        my $measurement_collection = $database->get_collection( 'measurements' );

        $start = $measurement_collection->find( {}, {'start' => 1} )->sort( {'start' => 1 } )->limit( 1 )->next();

        # ISSUE=9951 no measurements found
        if ( !$start ) {

            $start = $end;
        }

        else {

            $start = $start->{'start'};
        }
    }

    # determine aggregate data collection name for this aggregate
    my $collection_name = $self->_get_collection_name( aggregate => $aggregate );

    # truncate to the correct start/end timestamps of the buckets within the range we'll need to aggregate
    $start = nlowmult( $aggregate_seconds, $start );
    $end = nhimult( $aggregate_seconds, $end );

    my $total_buckets = ( $end - $start ) / $aggregate_seconds;
    my $bucket_num = 0;

    my $cli = GRNOC::CLI->new();

    $cli->start_progress( $total_buckets ) if !$self->quiet;

    my $num_done = 0;

    # only fetch the necessary hires data for one single bucket at a time
    for ( my $i = $end; $i > $start; $i -= $aggregate_seconds ) {
        #warn "on $i/$start";
        $num_done++;

        # we need to know the start and end of this bucket time period
        my $current_start = $i - $aggregate_seconds;
        my $current_end = $i;

        # we also need to know the start and end of the aggregate document this bucket is contained within
        my $doc_start = nlowmult( $aggregate_seconds * AGGREGATE_DOCUMENT_SIZE, $current_start );
        my $doc_end = $doc_start + ( $aggregate_seconds * AGGREGATE_DOCUMENT_SIZE );

        $self->logger->debug( "$database->{'name'} $aggregate->{'name'}: $num_done / $total_buckets [$current_start => $current_end]" );
        $self->logger->debug( "Aggregate document start: $doc_start end: $doc_end" );

        $cli->progress_message( "$database->{'name'} $aggregate->{'name'}: $num_done / $total_buckets [$current_start => $current_end]" ) if !$self->quiet;

        my @identifiers = keys( %$measurements );
        my $num_measurements = @identifiers;
        my $num_per_process = ceil( $num_measurements / $self->num_processes );

        # no identifiers to process at this aggregation bucket
        if ( $num_measurements == 0 ) {

            $cli->update_progress( $num_done ) if !$self->quiet;
            next;
        }

        # create forker object w/ number of desired max processes, if needed
        my $forker;

        if ( $self->num_processes > 1 ) {

            $forker = Parallel::ForkManager->new( $self->num_processes );
        }

        # evenly split number of measurements to handle per each process
        my $it = natatime( $num_per_process, @identifiers );

        # get the next set of identifiers to handle for the next child process
        while ( my @chunk = $it->() ) {

            if ( $forker ) {

                $forker->start() and next;
            }

            # we'll need a brand new connection to mongo since we're in a new process
            $self->_mongo_connect() if $forker;

            $database = $self->mongo_rw()->get_database( $database->{'name'} );

            # get ahold of the hires data collection
            my $hires_collection = $database->get_collection( 'data' );

            # get ahold of the aggregate collection
            my $aggregate_data_collection = $database->get_collection( $collection_name );

            my $found_bulk = 0;
            my $bulk = $aggregate_data_collection->initialize_unordered_bulk_op();

            # handle each identifier one at a time in this child process
            foreach my $identifier ( @chunk ) {

                my $query = Tie::IxHash->new( identifier => $identifier,
                                              start => $doc_start,
                                              end => $doc_end );

                # get all the highres values for this bucket time period
                my $hires_values = $self->_get_bucket_hires_values( identifier => $identifier,
                                                                    start => $current_start,
                                                                    end => $current_end,
                                                                    collection => $hires_collection );

                # no data found during this time period
                next if ( keys( %$hires_values ) == 0 );

                my $bucket_entry = $self->_get_bucket_entry( aggregate => $aggregate,
                                                             values => $hires_values,
                                                             measurement => $measurements->{$identifier} );

                my @types = keys( %$bucket_entry );

                # retrieve the old aggregate document (if any)
                my $existing_document = $self->_get_aggregate_document( collection => $aggregate_data_collection,
                                                                        identifier => $identifier,
                                                                        start => $doc_start,
                                                                        end => $doc_end );

                # document doesn't exist?
                if ( !defined( $existing_document ) ) {

                    $self->logger->debug( "Creating new aggregate document identifier: $identifier start: $doc_start end: $doc_end" );

                    # insert new document since it doesn't exist
                    my $doc = $self->_generate_aggregate_document( collection => $aggregate_data_collection,
                                                                   identifier => $identifier,
                                                                   start => $doc_start,
                                                                   end => $doc_end,
                                                                   types => \@types,
                                                                   updated => time(),
                                                                   interval => $aggregate_seconds );

                    $aggregate_data_collection->insert( $doc ) if !$self->pretend;
                }

                # make sure all of our types exist in the existing document
                else {

                    foreach my $type ( @types ) {

                        # is this type missing from the old doc?
                        if ( !defined( $existing_document->{'values'}{$type} ) ) {

                            $self->logger->debug( "Creating new type $type in aggregate document identifier: $identifier start: $doc_start end: $doc_end" );

                            my $array = $self->_create_aggregate_array();

                            $bulk->find( $query )->update( {'$set' => {"values.$type" => $array}} );
                            $found_bulk = 1;
                        }
                    }
                }

                # at which indicies in the array is this bucket?
                my ( $x, $y, $z ) = $self->_get_indexes( start => $doc_start,
                                                         time => $current_start,
                                                         interval => $aggregate_seconds );

                # handle every type of data within this measurement
                foreach my $type ( @types ) {

                    my $data_entry = $bucket_entry->{$type};

                    # set the bucket data for this type
                    $bulk->find( $query )->update( {'$set' => {"values.$type.$x.$y.$z" => $data_entry}} );
                    $found_bulk = 1;
                }
            }

            $bulk->execute() if ( !$self->pretend && $found_bulk );
            $forker->finish() if $forker;
        }

        $self->logger->debug( 'Waiting for all child worker processes to exit.' );

        # wait until all child processes have finished
        $forker->wait_all_children() if $forker;

        $self->logger->debug( 'All child workers have exited.' );

        $cli->update_progress( $num_done ) if !$self->quiet;
    }

    # was this an automated 'full run' of this aggregate and not a manual custom timeframe or element?
    if ( $self->is_automatic_run ) {

        # set our last run to the the start time of the last bucket we aggregated
        my $last_run = $end - $aggregate_seconds;

        $self->logger()->debug( "Updating last_run of aggregate $aggregate->{'name'} in database $database->{'name'} to $end." );

        # update the last successful run of this aggregate
        my $aggregate_collection = $database->get_collection( 'aggregate' );

        $aggregate_collection->update( {'name' => $aggregate->{'name'}},
                                       {'$set' => {'last_run' => $last_run}} );

        # remove our lock for this aggregate
        $self->locker->unlock( $lock );
    }

    return 1;
}

sub _generate_aggregate_document {

    my ( $self, %args ) = @_;

    my $collection = $args{'collection'};
    my $identifier = $args{'identifier'};
    my $start = $args{'start'};
    my $end = $args{'end'};
    my $types = $args{'types'};
    my $interval = $args{'interval'};
    my $updated = $args{'updated'};

    my $values = {};

    foreach my $type ( @$types ) {

        my $array = $self->_create_aggregate_array();

        $values->{$type} = $array;
    }

    return {'identifier' => $identifier,
            'start' => $start,
            'end' => $end,
            'updated' => $updated,
            'values' => $values,
            'interval' => $interval};
}

sub _create_aggregate_array {

    my ( $self, $interval ) = @_;

    my $array = [];

    my ( $size_x, $size_y, $size_z ) = $self->_get_dimensions( $interval );

    for ( my $i = 0; $i < $size_x; $i++ ) {

        for ( my $j = 0; $j < $size_y; $j++ ) {

            for ( my $k = 0; $k < $size_z; $k++ ) {

                $array->[$i][$j][$k] = undef;
            }
        }
    }

    return $array;
}

sub _get_dimensions {

    my ( $self, $interval ) = @_;

    return ( 10, 10, 10 );

#    my $size = AGGREGATE_DOCUMENT_SIZE;

#    my %factors = ();
#    my $d = 2;

#    while ( $size > 1 ) {

#        while ( $size % $d == 0 ) {

#            $factors{$d} = ( $factors{$d} || 0 ) + 1;
#            $size = $size / $d;
#        }

#        $d++;
#    }

#    my @uniqued = ();

#    my $count = scalar( keys( %factors ) );

#    if ( $count < 3 ) {

#        foreach my $div ( keys( %factors ) ) {

#            if ( $factors{$div} > 1 ) {

#                push( @uniqued, $div );
#                $factors{$div} -= 1;
#                last;
#            }
#        }
#    }

#    foreach my $factor ( keys( %factors ) ) {

#        my $to_append = $factor ** $factors{$factor};

#        push( @uniqued, $factor ** $factors{$factor} );
#    }

#    return @uniqued;
}

sub _get_indexes {

    my ( $self, %args ) = @_;

    my $start = $args{'start'};
    my $interval = $args{'interval'};
    my $time = $args{'time'};

    my @dimensions = $self->_get_dimensions( $interval );

    # align time to interval
    $time = int( $time / $interval ) * $interval;

    my $diff = ( $time - $start ) / $interval;

    my ( $size_x, $size_y, $size_z ) = @dimensions;

    my $x = int( $diff / ( $size_y * $size_z ) );
    my $remainder = $diff - ( $size_y * $size_z * $x );
    my $y = int( $remainder / $size_z );
    my $z = $remainder % $size_z;

    return ( $x, $y, $z );
}

sub _get_aggregate_document {

    my ( $self, %args ) = @_;

    my $collection = $args{'collection'};
    my $identifier = $args{'identifier'};
    my $start = $args{'start'};
    my $end = $args{'end'};

    my $doc = $collection->find( {'identifier' => $identifier,
                                  'start' => $start,
                                  'end' => $end} )->hint( 'identifier_1_start_1_end_1' )->limit( 1 )->next();

    return $doc;
}

sub _get_bucket_offset {

    my ( $self, %args ) = @_;

    my $aggregate_start = $args{'aggregate_start'};
    my $bucket_start = $args{'bucket_start'};
    my $aggregate_seconds = $args{'aggregate_seconds'};

    return ( $bucket_start - $aggregate_start ) / $aggregate_seconds;
}

sub _get_bucket_entry {

    my ( $self, %args ) = @_;

    my $aggregate = $args{'aggregate'};
    my $bucket = $args{'values'};
    my $measurement = $args{'measurement'};

    my $result = {};

    my @types = keys( %$bucket );

    # handle every value type
    foreach my $type ( @types ) {

        my $min;
        my $max;
        my $sum;
        my $count;
        my $avg;
        my $hist;

        # handle every value in this type
        foreach my $value ( @{$bucket->{$type}} ) {

            # initialize total count, sum, min, max if needed
            $count = 0 if ( !defined( $count ) );
            $sum = 0 if ( !defined( $sum ) );
            $min = $value->{'value'} if ( !defined( $min ) );
            $max = $value->{'value'} if ( !defined( $max ) );

            # determine new sum for our average calculation
            $sum += $value->{'value'};
            $count++;

            # determine if there is a new min/max
            $min = $value->{'value'} if ( $value->{'value'} < $min );
            $max = $value->{'value'} if ( $value->{'value'} > $max );
        }

        # we have the min, max, and sum, but we also need the mean/avg
        $avg = $sum / $count if $count;

        # generate our percentile histogram between min => max
        if ( defined( $min ) && defined( $max ) && $min != $max ) {

            # get the histogram attributes of the aggregation for this value type
            my $resolution = $aggregate->{'values'}{$type}{'hist_res'};
            my $min_width = $aggregate->{'values'}{$type}{'hist_min_width'};

            if ( $resolution && $min_width ) {

                # get the smallest/greatest possible min/max of this value type for this measurement
                my $absolute_min = $measurement->{'values'}{$type}{'min'};
                my $absolute_max = $measurement->{'values'}{$type}{'max'};

                $hist = GRNOC::TSDS::Aggregate::Histogram->new( hist_min => $absolute_min,
                                                                hist_max => $absolute_max,
                                                                data_min => $min,
                                                                data_max => $max,
                                                                min_width => $min_width,
                                                                resolution => $resolution );
            }

            if ( defined( $hist ) ) {

                my @values;

                # add every value into our histogram
                foreach my $value ( @{$bucket->{$type}} ) {

                    push( @values, $value->{'value'} );
                }

                $hist->add_values( \@values );

                $hist = {'total' => $hist->total(),
                         'bin_size' => $hist->bin_size(),
                         'num_bins' => $hist->num_bins(),
                         'min' => $hist->hist_min(),
                         'max' => $hist->hist_max(),
                         'bins' => $hist->bins()};
            }
        }

        # all done handling the aggregation of this data type
        $result->{$type} = {'min' => $min,
                            'max' => $max,
                            'avg' => $avg,
                            'hist' => $hist};
    }

    return $result;
}

sub _get_bucket_hires_values {

    my ( $self, %args ) = @_;

    my $identifier = $args{'identifier'};
    my $aggregate_start = $args{'start'};
    my $aggregate_end = $args{'end'};
    my $collection = $args{'collection'};

    # final results to return, which contains buckets based upon aggregate duration length
    my $results = [];

    # flat array of all values for every type
    my $type_values = {};

    my $fetched_docs = [];

    # have we already grabbed the docs this aggregate interval is contained within?
    if ( defined $self->{'last'}{'start'} && $identifier eq $self->{'last'}{'identifier'} && $aggregate_start >= $self->{'last'}{'start'} && $aggregate_end <= $self->{'last'}{'end'} ) {

        $fetched_docs = $self->{'last'}{'docs'};
    }

    else {

        # clear out last docs
        delete( $self->{'last'} );

        # set this identifier
        $self->{'last'}{'identifier'} = $identifier;

        # find all high res data documents that contain some data points during this aggregate period
        # help from http://eli.thegreenplace.net/2008/08/15/intersection-of-1d-segments
        my $query = Tie::IxHash->new( 'identifier' => $identifier,
                                      'start' => {'$lt' => $aggregate_end},
                                      'end' => {'$gt' => $aggregate_start} );

        # make sure we order by the identifier of the documents in ascending order according to time;
        # we rely upon the data being ordered correctly in ascending order
        my $sort = Tie::IxHash->new( 'identifier' => 1,
                                     'start' => 1 );

        # get all of the hires doc data during this entire aggregation period
        my $hires_docs = $collection->find( $query )->hint( 'identifier_1_start_1_end_1' )->sort( $sort );

        while ( my $doc = $hires_docs->next ) {

            # set the start time for these docs if needed
            $self->{'last'}{'start'} = $doc->{'start'} if ( !defined( $self->{'last'}{'start'} ) );

            # update the last time for these docs
            $self->{'last'}{'end'} = $doc->{'end'};

            push( @$fetched_docs, $doc );
        }

        # update the last docs we've found
        $self->{'last'}{'docs'} = $fetched_docs;
    }

    # handle every document necessary within the aggregation period
    foreach my $doc ( @$fetched_docs ) {

        my $interval = $doc->{'interval'};
        my @value_types = keys( %{$doc->{'values'}} );

        foreach my $value_type ( @value_types ) {

            my $timestamp = $doc->{'start'};

            # initialize array for this type if needed
            $type_values->{$value_type} = [] if ( !defined( $type_values->{$value_type} ) );

            my $values = $doc->{'values'}{$value_type};

            # this is a 3d array
            if ( ref( $values->[0] ) ) {

                foreach my $x ( @$values ) {

                    last if ( $timestamp >= $aggregate_end );

                    foreach my $y ( @$x ) {

                        last if ( $timestamp >= $aggregate_end );

                        foreach my $value ( @$y ) {

                            last if ( $timestamp >= $aggregate_end );

                            # only handle this value if it occurs within our aggregate timeframe
                            if ( defined( $value ) && $timestamp >= $aggregate_start && $timestamp < $aggregate_end ) {

                                push( @{$type_values->{$value_type}}, {'timestamp' => $timestamp,
                                                                       'value' => $value} );
                            }

                            # determine timestamp of next value we look at
                            $timestamp += $interval;
                        }
                    }
                }
            }

            # this is a 1d array
            else {

                foreach my $value ( @$values ) {

                    last if ( $timestamp >= $aggregate_end );

                    # only handle this value if it occurs within our aggregate timeframe
                    if ( defined( $value ) && $timestamp >= $aggregate_start && $timestamp < $aggregate_end ) {

                        push( @{$type_values->{$value_type}}, {'timestamp' => $timestamp,
                                                               'value' => $value} );
                    }

                    # determine timestamp of next value we look at
                    $timestamp += $interval;
                }
            }
        }
    }

    return $type_values;
}

sub _get_collection_size {

    my ( $self, %args ) = @_;

    my $database = $args{'database'};
    my $collection = $args{'collection'};

    return $database->run_command( ['collStats' => $collection->{'name'}] )->{'size'};
}

sub _get_collection_name {

    my ( $self, %args ) = @_;

    my $aggregate = $args{'aggregate'};

    my $collection_name = 'data';

    $collection_name .= "_$aggregate->{'interval'}" if ( defined( $aggregate->{'interval'} ) );

    return $collection_name;
}

sub _mongo_connect {

    my ( $self ) = @_;

    my $mongo_host = $self->config->get( '/config/mongo/@host' );
    my $mongo_port = $self->config->get( '/config/mongo/@port' );
    my $rw_user    = $self->config->get( "/config/mongo/readwrite" );

    $self->logger->debug( "Connecting to MongoDB as readwrite on $mongo_host:$mongo_port." );

    my $mongo;
    eval {
        $mongo = MongoDB::MongoClient->new(
            host => "$mongo_host:$mongo_port",
            query_timeout => -1,
            username => $rw_user->{'user'},
            password => $rw_user->{'password'}
            );
    };
    if($@){
        die "Could not connect to Mongo: $@";
    }

    $self->_set_mongo_rw( $mongo );
}

1;
