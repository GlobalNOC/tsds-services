package GRNOC::TSDS::Expire;

use Moo;

use GRNOC::Config;
use GRNOC::CLI;
use GRNOC::Log;

use GRNOC::TSDS::Constants;

use MongoDB;
use Tie::IxHash;
use JSON::XS;
use Math::Round qw( nlowmult nhimult );
use boolean;

use Data::Dumper;

### required attributes ###

has config_file => ( is => 'ro',
                     required => 1 );

has logging_file => ( is => 'ro',
                      required => 1 );

### optional attributes ###

has start => ( is => 'ro',
               required => 0 );

has end => ( is => 'ro',
             required => 0 );

has database => ( is => 'ro',
                  required => 0 );

has expire => ( is => 'rw',
                required => 0 );

has query => ( is => 'ro',
               required => 0 );

has pretend => ( is => 'ro',
                 default => 0 );

### internal attributes ###

has config => ( is => 'rwp' );

has logger => ( is => 'rwp' );

has mongo_rw => ( is => 'rwp' );

has json => ( is => 'rwp' );

has now => ( is => 'rwp' );

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

    # connect to mongo
    $self->_mongo_connect();

    # determine current timestamp
    $self->_set_now( time() );

    return $self;
}

### public methods ###

sub expire_data {

    my ( $self ) = @_;

    # determine all possible databases we need to handle
    my @database_names = $self->mongo_rw()->database_names();

    my $total_expired = 0;

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

        # expire all necessary data for this database
        my $num_expired = $self->_expire_database( $database );

        $total_expired += $num_expired;
    }

    return $total_expired;
}

### private methods ###

sub _expire_database {

    my ( $self, $database ) = @_;

    # get the expire collection
    my $expire_collection = $database->get_collection( 'expire' );
    my $measurement_collection = $database->get_collection( 'measurements' );

    # get all configured expirations for this database, sorted by interval + eval position
    my $sort = Tie::IxHash->new( interval => 1,
                                 eval_position => 1 );

    my @expires = $expire_collection->find( {} )->sort( $sort )->all();

    # map expiration name to expiration
    my $expire_index = {};

    # interval + expiration + measurement
    my $expire_measurement_mappings = {};

    # only handle the most recent measurement entry at each interval for each identifier
    my $found_identifiers = {};

    my $total_num_removed = 0;

    foreach my $expire ( @expires ) {

        my $name = $expire->{'name'};
        my $interval = $expire->{'interval'} || "";
        my $meta = $expire->{'meta'};
        my $user_query = $self->query;

        # skip it if they specified an expiration and its not this one
        next if ( defined $self->expire && $name ne $self->expire );

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

        $expire_index->{$name} = $expire;

        # first time we've handled this interval
        $expire_measurement_mappings->{$interval} = {} if ( !$expire_measurement_mappings->{$interval} );

        # first time we've handled this interval + expiration
        $expire_measurement_mappings->{$interval}{$name} = {} if ( !$expire_measurement_mappings->{$interval}{$name} );

        # find the measurements which match this expire entry
        my $measurements = $measurement_collection->find( $query );

        while ( my $measurement = $measurements->next() ) {

            my $identifier = $measurement->{'identifier'};
            my $start = $measurement->{'start'};

            my $prior_start = $found_identifiers->{$interval}{$identifier};

            # we've previously handled this identifier before for an expiration at this interval
            if ( defined $prior_start ) {

                # is this measurement earlier than the one we've already seen for it?
                if ( $start < $prior_start ) {

                    # skip this measurement in this expiration as there is a later measurement entry for it
                    next;
                }
            }

            # mark this measurement identifier as being handled at this interval + time
            $found_identifiers->{$interval}{$identifier} = $start;

            # remove this measurement as being part of any other expirations at this interval
            my @others = keys( %{$expire_measurement_mappings->{$interval}} );

            foreach my $other ( @others ) {

                delete( $expire_measurement_mappings->{$interval}{$other}{$identifier} );
            }

            # mark this expiration for this measurement at this interval to be used
            $expire_measurement_mappings->{$interval}{$name}{$identifier} = $measurement;
        }
    }

    my @intervals = keys( %$expire_measurement_mappings );

    foreach my $interval ( @intervals ) {

        my @expires = keys( %{$expire_measurement_mappings->{$interval}} );

        foreach my $expire ( @expires ) {

            my $measurements = $expire_measurement_mappings->{$interval}{$expire};

            my $num_removed = $self->_expire( database => $database,
					      expire => $expire_index->{$expire},
					      measurements => $measurements );

	    $total_num_removed += $num_removed;
        }
    }

    return $total_num_removed;
}

sub _expire {

    my ( $self, %args ) = @_;

    my $database = $args{'database'};
    my $expire = $args{'expire'};
    my $measurements = $args{'measurements'};

    my @identifiers = keys( %$measurements );

    my $start = $self->start;
    my $end = $self->end;

    my $max_age = $expire->{'max_age'};
    my $interval = $expire->{'interval'};

    # determine which data collection to remove from
    my $data_collection = $self->_get_data_collection( database => $database,
                                                       expire => $expire );

    my $query = Tie::IxHash->new();

    $query->Push( identifier => {'$in' => \@identifiers} );

    # did they specify a custom timeframe?
    if ( defined( $start ) || defined( $end ) ) {

        # did they specify a start time?
        if ( defined( $start ) ) {

            # is this for the hires default docs?
            if ( $data_collection->name eq 'data' ) {

                # scale timestamp to the start time of the next hires document
                $start = nhimult( HIGH_RESOLUTION_DOCUMENT_SIZE, $start );
            }

            else {

                # scale timestamp to the start time of the next aggregate document
                $start = nhimult( $interval * AGGREGATE_DOCUMENT_SIZE, $start );
            }

            $query->Push( start => {'$gte' => $start} );
        }

        # did they specify a end time?
        if ( defined( $end ) ) {

            # is this for the hires default docs?
            if ( $data_collection->name eq 'data' ) {

                # scale timestamp to the start time of the specified hires document
                $end = nlowmult( HIGH_RESOLUTION_DOCUMENT_SIZE, $end );
            }

            else {

                # scale timestamp to the start time of the specified aggregate document
                $end = nlowmult( $interval * AGGREGATE_DOCUMENT_SIZE, $end );
            }

            $query->Push( end => {'$lte' => $end} );
        }
    }

    # no custom timeframe given, use the expiration policy max age
    else {

	# max age must be specified
	return 0 if ( !$max_age );

        # what is the minimum timestamp of the data based upon configured maximum age
        my $minimum_timestamp = $self->now - $max_age;

        # is this the raw hires data collection?
        if ( $data_collection->name eq 'data' ) {

            # scale timestamp to the start time of the hires document
            $minimum_timestamp = nlowmult( HIGH_RESOLUTION_DOCUMENT_SIZE, $minimum_timestamp );
        }

        # this is an aggregate collection
        else {

            # scale timestamp to the start time of the aggregate document
            $minimum_timestamp = nlowmult( $interval * AGGREGATE_DOCUMENT_SIZE, $minimum_timestamp );
        }

        $query->Push( start => {'$lt' => $minimum_timestamp} );
    }

    # only remove things if not in pretend mode
    if ( !$self->pretend ) {

        my $ret = $data_collection->remove( $query,
                                            {'safe' => true,
                                             'just_one' => false} );

        # determine how many documents were removed
        my $num_removed = $ret->{'n'};

        $self->logger()->info( "Removed $num_removed documents from the $data_collection->{'name'} collection in the $database->{'name'} database." );

        return $num_removed;
    }

    return 0;
}

sub _get_data_collection {

    my ( $self, %args ) = @_;

    my $database = $args{'database'};
    my $expire = $args{'expire'};

    # default highres data
    my $collection_name = 'data';

    # was it an expiration for an aggregate?
    $collection_name .= "_$expire->{'interval'}" if ( $expire->{'interval'} );

    return $database->get_collection( $collection_name );
}

sub _mongo_connect {

    my ( $self ) = @_;

    my $mongo_host = $self->config->get( '/config/mongo/@host' );
    my $mongo_port = $self->config->get( '/config/mongo/@port' );
    my $rw_user    = $self->config->get( "/config/mongo/readwrite" );

    $self->logger->debug( "Connecting to MongoDB $mongo_host:$mongo_port." );
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
