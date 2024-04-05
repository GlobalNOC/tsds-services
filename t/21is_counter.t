use strict;
use warnings;

use Test::More tests => 4;

use GRNOC::Config;
use GRNOC::TSDS::Writer;
use GRNOC::TSDS::Cache;
use GRNOC::TSDS::DataDocument;
use GRNOC::TSDS::DataType;

use Net::AMQP::RabbitMQ;
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


my $metadata = $cache->get_data_type_required_metadata("tsdstest");
ok(@$metadata == 2 && $metadata->[0] eq "intf" && $metadata->[1] eq "node" , "Got expected metadata");


# Bad measurement type queried
my $bad_metadata = $cache->get_data_type_required_metadata("tsdstestbad");
ok(!defined $bad_metadata, "Got expected metadata (undef)");


#
# $cache->get_data_type_counter_values
#


#
# $cache->get_indexes
#

#
# $cache->get_prev_measurement_values
#

#
# $cache->set_measurement_values
#
