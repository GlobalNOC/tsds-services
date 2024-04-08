use strict;
use warnings;

use Test::More tests => 13;

use GRNOC::Config;
use GRNOC::TSDS::Writer;
use GRNOC::TSDS::Cache;
use GRNOC::TSDS::DataDocument;
use GRNOC::TSDS::DataType;

use Net::AMQP::RabbitMQ;
use Test::Deep;
use JSON::XS;

use FindBin;
use Data::Dumper;


my $config_file = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";


my $writer = new GRNOC::TSDS::Writer(
    config_file => $config_file,
    logging_file => $logging_file,
    daemonize => 0
);

my $config = new GRNOC::Config(
    config_file => $config_file,
    force_array => 0
);

my $cache = new GRNOC::TSDS::Cache(
    config_file => $config_file
);
$cache->cache_timeout = 1;


#
# $cache->measurement_id
#


my $measurement = {
    interval => 60,
    meta => {
        node => "node1",
        intf => "intf1"
    },
    time => 1437072299,
    type => "tsdstest",
    values => {
        value => 42
    }
};

my $measurement_id = $cache->measurement_id($measurement);
ok($measurement_id eq "a7924184bb16e860323019d370b347d501c9e32d78eee7a84da3b57b6c02b6ad", "Got expected measurement_id");


# Required metadata is missing
my $bad_measurement = {
    interval => 60,
    meta => {
        location => "eu",
        server =>   "srv1"
    },
    time => 1437072299,
    type => "tsdstest",
    values => {
        value => 42
    }
};

my $bad_measurement_id = $cache->measurement_id($bad_measurement);
ok(!defined $bad_measurement_id, "Got undef measurement_id when missing req fields");


#
# $cache->get_data_type_required_metadata
#


$cache->cache_timeout(3);
my $metadata = $cache->get_data_type_required_metadata("tsdstest");
ok(@$metadata == 2 && $metadata->[0] eq "intf" && $metadata->[1] eq "node" , "Got expected metadata");

# Second call to ensure cached metadata also returned correctly
$metadata = $cache->get_data_type_required_metadata("tsdstest");
ok(@$metadata == 2 && $metadata->[0] eq "intf" && $metadata->[1] eq "node" , "Got expected metadata");

# Bad measurement type queried
my $bad_metadata = $cache->get_data_type_required_metadata("tsdstestbad");
ok(!defined $bad_metadata, "Got expected metadata (undef)");


#
# $cache->get_data_type_counter_values
#


$cache->cache_timeout(3);
my $expected_counters = {
    'input' => 0,
    'output' => 0,
};
my $counters = $cache->get_data_type_counter_values("tsdstest");
cmp_deeply($counters, $expected_counters, "Got expected counters");

# Second call to ensure cached values are also returned correctly
$counters = $cache->get_data_type_counter_values("tsdstest");
cmp_deeply($counters, $expected_counters, "Got expected counters");


#
# $cache->get_indexes
#


my $i = $cache->get_indexes(timestamp => 1422000000, start => 1422000000, end => 1425600000, interval => 3600);
ok($i->[0] == 0 && $i->[1] == 0 && $i->[2] == 0, "Got expected indexes");

$i = $cache->get_indexes(timestamp => 1422000000 + 3600, start => 1422000000, end => 1425600000, interval => 3600);
ok($i->[0] == 0 && $i->[1] == 0 && $i->[2] == 1, "Got expected indexes");

$i = $cache->get_indexes(timestamp => 1422000000 + (3600*10), start => 1422000000, end => 1425600000, interval => 3600);
ok($i->[0] == 0 && $i->[1] == 1 && $i->[2] == 0, "Got expected indexes");

$i = $cache->get_indexes(timestamp => 1422000000 + (3600*100), start => 1422000000, end => 1425600000, interval => 3600);
ok($i->[0] == 1 && $i->[1] == 0 && $i->[2] == 0, "Got expected indexes");


#
# $cache->get_prev_measurement_values
#


$cache->cache_timeout(3);
my $new_measurement = {
    interval => 10,
    meta => {
        node => "rtr.chic",
        intf => "interface5",
    },
    time => 10,
    type => "tsdstest",
    values => {
        value => 42
    }
};
my $expected_values = {
    'input' => 43201,
    'output' => 43201,
};
my $prev_values = $cache->get_prev_measurement_values($new_measurement);
cmp_deeply($prev_values, $expected_values, "Got expected values");


#
# $cache->set_measurement_values
#


$cache->cache_timeout(3);
my $new_measurement2 = {
    interval => 10,
    meta => {
        node => "rtr.chic",
        intf => "interface5",
    },
    time => 10,
    type => "tsdstest",
    values => {
        value => 42
    }
};
my $result = $cache->set_measurement_values($new_measurement2);

my $measurement_id2 = $cache->measurement_id($new_measurement2);
my $cache_id = "measurement:$measurement_id2:10";

my %cached_values = $cache->redis->hgetall($cache_id);
cmp_deeply(\%cached_values, $new_measurement2->{values}, "Cached expected values at expected key");
