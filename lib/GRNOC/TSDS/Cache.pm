#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::Cache;


use Data::Dumper;
use GRNOC::Log;
use GRNOC::TSDS::Config;
use GRNOC::TSDS::DataType;
use GRNOC::TSDS::Redis;
use InfluxDB::LineProtocol  qw(line2data);
use MongoDB;
use Moo;
use Redis::DistLock;
use Try::Tiny;
use Types::Standard qw( Str Object );

use Digest::SHA;

has config_file => (
    is => 'ro',
    isa => Str
);
has config => (
    is => 'rw',
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
has cache_timeout => (
    is => 'rw',
    default => 360*4
);

sub BUILD {
    my ($self, $args) = @_;

    if (defined $args->{config}) {
        $self->config($args->{config});
    } else {
        $self->config(new GRNOC::TSDS::Config(
            config_file => $args->{'config_file'}
        ));
    }

    my $mongo_conn = new GRNOC::TSDS::MongoDB(config => $self->config);
    if (!defined $mongo_conn) {
	    die "Error connecting to MongoDB. See logs for more details.";
    }
    $self->mongo($mongo_conn->mongo);

    try {
        my $redis_conn = new GRNOC::TSDS::Redis(config => $self->config);
        $self->redis($redis_conn->redis);
    }
    catch {
        log_error("Error connecting to Redis: $_ $@");
        die("Error connecting to Redis: $_");
    };

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
        log_error("Couldn't set measurement values for $cache_id.");
        return;
    }

    my $ok = $self->redis->expire($cache_id, $measurement->{interval} * 3);
    if (!$ok) {
        log_warn("Couldn't set TTL on $cache_id");
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
    log_debug("Previous values for $cache_id not cached. Checking database.");

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


=head2 get_data_type_counter_values

=cut
sub get_data_type_counter_values {
    my $self = shift;
    my $data_type_str = shift;

    my $counters_cache_id = "measurement_type_counter_values:$data_type_str";

    my %cached_counters = $self->redis->hgetall($counters_cache_id);
    if (%cached_counters) {
        return \%cached_counters;
    }
    log_debug("Counters for $data_type_str not found in cache. Fetching from db.");

    my $data_type = GRNOC::TSDS::DataType->new(
        name => $data_type_str,
        database => $self->mongo->get_database($data_type_str)
    );

    my $value_types = $data_type->value_types;
    my $counters = {};
    foreach my $key (keys %{$value_types}) {
        if ($value_types->{$key}->{is_counter}) {
            $counters->{$key} = 1;
        } else {
            $counters->{$key} = 0;
        }
    }
    my $result = $self->redis->hmset($counters_cache_id, %{$counters});
    if (!$result) {
        log_warn("Couldn't cache counters for $data_type_str.");
    }
    my $ok = $self->redis->expire(
        $counters_cache_id,
        $self->cache_timeout
    );
    if (!$ok) {
        log_warn("Couldn't set TTL on $counters_cache_id");
    }

    return $counters;
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
    log_debug("Metadata for $data_type_str not found in cache. Fetching from db.");

    # Try to load required metadata fields from db
    my @sorted_fields;
    eval {
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
        @sorted_fields = sort(@req_fields);
    };
    if ($@) {
        log_error($@);
        return;
    };

    # Cache required metadata fields
    my $count = $self->redis->rpush($cache_id, @sorted_fields);
    if (!$count) {
        log_warn("Couldn't cache required metadata fields for $data_type_str.");
    }
    my $ok = $self->redis->expire(
        $cache_id,
        $self->cache_timeout
    );
    if (!$ok) {
        log_warn("Couldn't set TTL on $cache_id");
    }

    return \@sorted_fields;
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
        log_error("Couldn't fetch required metadata fields for $measurement->{type}.");
        return;
    }

    my $hash = Digest::SHA->new(256);
    foreach my $field (@$req_fields) {
        if (!defined $measurement->{meta}->{$field}) {
            log_error("Measurement of type '$measurement->{type}' missing required metadata '$field'");
            return;
        }
        $hash->add($measurement->{meta}->{$field});
    }
    return $hash->hexdigest();
}

1;
