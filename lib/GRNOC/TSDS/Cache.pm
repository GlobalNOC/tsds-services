#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::Cache;


use Data::Dumper;
use GRNOC::Config;
use GRNOC::Log;
use GRNOC::TSDS::DataType;
use InfluxDB::LineProtocol  qw(line2data);
use MongoDB;
use Moo;
use Redis;
use Redis::DistLock;
use Try::Tiny;
use Types::Standard qw( Str Object );

use Digest::SHA;

has config_file => (
    is => 'ro',
    isa => Str,
    required => 1
);
has config => (
    is => 'ro',
    isa => Object
);
has redis => (
    is => 'rw',
    isa => Object
);
has mongo => (
    is => 'rw',
    isa => Object
);
has locker => (
    is => 'rw',
    isa => Object
);


sub BUILD {
    my ($self, $args) = @_;

    $self->{config} = new GRNOC::Config(
        config_file => $args->{config_file},
        force_array => 0
    );

    my $mongo_host = $self->config->get('/config/mongo/@host');
    my $mongo_port = $self->config->get('/config/mongo/@port');
    my $rw_user    = $self->config->get('/config/mongo/readwrite');
    log_debug("Connecting to MongoDB as readwrite on $mongo_host:$mongo_port.");
    warn "Connecting to MongoDB as readwrite on $mongo_host:$mongo_port.";

    try {
        $self->mongo(new MongoDB::MongoClient(
            host => "$mongo_host:$mongo_port",
            username => $rw_user->{'user'},
            password => $rw_user->{'password'}
        ));
    }
    catch {
        log_error("Error connecting to MongoDB: $_");
        die("Error connecting to MongoDB: $_");
    };

    my $redis_host = $self->config->get('/config/redis/@host');
    my $redis_port = $self->config->get('/config/redis/@port');

    try {
        log_debug("Connecting to Redis $redis_host:$redis_port.");
        $self->redis(new Redis(
            server => "$redis_host:$redis_port",
            reconnect => 120,
            every => 3 * 1000 * 1000
        )); # microseconds
    }
    catch {
        log_error("Error connecting to Redis: $_ $@");
        die("Error connecting to Redis: $_");
    };

    log_debug('Creating locker.');
    $self->locker(Redis::DistLock->new(
        servers => [$self->redis],
        retry_count => 3,
        retry_delay => 0.5
    ));

    return $self;
}


=head2 set_measurement_values

Store the values of `measurement` in redis under the key
`measurement:$id:$timestamp`.

    measurement:123:987654321
      input:  1234
      output: 5678

=cut
sub set_measurement_values {
    my $self = shift;
    my $measurement = shift;

    my $measurement_id = $self->measurement_id($measurement);
    my $cache_id = "measurement:$measurement_id:$measurement->{time}";
    my $result = $self->redis->hmset($cache_id, %{$measurement->{values}});
    if (!$result) {
        warn "Couldn't set measurement values for $cache_id.";
        return;
    }

    my $ok = $self->redis->expire($cache_id, $measurement->{interval} * 3);
    if (!$ok) {
        warn "Couldn't set TTL on $cache_id";
    }
    return $result;
}


=head2 get_prev_measurement_values

Store the values of `measurement` in redis under the key
`measurement:$id:$timestamp`.

    measurement:123:987654321
      input:  1234
      output: 5678

=cut
sub get_prev_measurement_values {
    my $self = shift;
    my $measurement = shift;

    my $measurement_id = $self->measurement_id($measurement);
    my $prev_timestamp = $measurement->{time} - $measurement->{interval};

    # Check cache for previous values
    my $cache_id = "measurement:$measurement_id:$prev_timestamp";
    my %prev_measurement = $self->redis->hgetall($cache_id);
    if (%prev_measurement) {
        return \%prev_measurement;
    }
    warn "Previous values for $cache_id not cached. Checking database.";

    my $collection = $self->mongo->get_database(
        $measurement->{type}
    )->get_collection('data');

    my $fields = $collection->find_one({
        '$and' => [
            {"start" => {'$lte' => $prev_timestamp}},
            {"end" => {'$gte' => $prev_timestamp}},
            {"identifier" => $measurement_id},
        ]
    });

    my $values = {};
    my $indexes = $self->get_indexes(
        timestamp => $prev_timestamp,
        start => $fields->{start},
        end => $fields->{end},
    );
    my ( $x, $y, $z ) = @$indexes;

    foreach my $key (keys %{$fields->{values}}) {
        $values->{$key} = $fields->{values}->{$key}->[$x]->[$y]->[$z];
    }

    # We do not cache these values as the current measurement will be
    # the prev_measurement on the next iteration.

    return $values;
}


sub get_indexes {
    my $self = shift;
    my $args = {
        timestamp => undef,
        start => undef,
        end => undef,
        interval => 60,
        @_
    };

    my $time = $args->{timestamp};
    my $start = $args->{start};
    my $end = $args->{end};
    my $interval = $args->{interval};
    my $dimensions = [10, 10, 10];

    # align time to interval
    $time = int( $time / $interval ) * $interval;

    my $diff = ( $time - $start ) / $interval;

    my ( $size_x, $size_y, $size_z ) = @$dimensions;

    my $x = int( $diff / ( $size_y * $size_z ) );
    my $remainder = $diff - ( $size_y * $size_z * $x );
    my $y = int( $remainder / $size_z );
    my $z = $remainder % $size_z;

    return [$x, $y, $z];
}


=head2 get_data_type_required_metadata

=cut
sub get_data_type_required_metadata {
    my $self = shift;
    my $data_type_str = shift;

    my $cache_id = "measurement_type_required_metadata:$data_type_str";

    # Try to load required metadata fields from cache
    my @result = $self->redis->lrange($cache_id, 0, -1);
    if (@result > 0) {
        return \@result;
    }
    warn Dumper("Metadata for $data_type_str not found in cache. Fetching from db.");

    # Try to load required metadata fields from db
    my $data_type = GRNOC::TSDS::DataType->new(
        name => $data_type_str,
        database => $self->mongo->get_database($data_type_str)
    );

    my $metadata = $data_type->metadata_fields;
    my @req_fields;
    foreach my $field (keys %{$metadata}) {
        if ($metadata->{$field}{required}) {
            push @req_fields, $field;
        }
    }
    my @sorted_fields = sort(@req_fields);

    # Cache required metadata fields
    my $count = $self->redis->rpush($cache_id, @sorted_fields);
    if (!$count) {
        warn "Couldn't cache required metadata fields for $data_type_str.";
        return;
    }
    my $ok = $self->redis->expire(
        $cache_id,
        360*24
    );
    if (!$ok) {
        warn "Couldn't set TTL on $cache_id";
    }

    return \@sorted_fields;
}


=head2 get_data_type

- get data type
- get measurement-type's requried metadata as a sorted list
- generate hash from measurement's required metadata values

=cut
sub get_data_type {
    my $self = shift;
    my $data_type_str = shift;

    my $data_type = GRNOC::TSDS::DataType->new(
        name => $data_type_str,
        database => $self->mongo->get_database($data_type_str)
    );
    return $data_type;
}


# We need some actual TSDS functions around here...
=head2 measurement_id

=cut
sub measurement_id {
    my $self = shift;
    my $measurement = shift;

    my $req_fields = $self->get_data_type_required_metadata(
        $measurement->{type}
    );
    if (!defined $req_fields) {
        warn "This is bad.";
        return;
    }

    my $hash = Digest::SHA->new(256);
    foreach my $field (@$req_fields) {
        $hash->add($measurement->{meta}->{$field});
    }
    return $hash->hexdigest();
}

1;