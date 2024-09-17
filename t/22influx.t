use strict;
use warnings;

use Test::More tests => 2;

use GRNOC::TSDS::InfluxDB;

use Net::AMQP::RabbitMQ;
use Test::Deep;
use JSON::XS;

use FindBin;
use Data::Dumper;


my $config_file = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";


my $influx = new GRNOC::TSDS::InfluxDB(
    config_file => $config_file
);

my $input1 = "temperature,interval=60,node=rtr.ipiu.ilight.net,name=temp1 temp=100i 1409670671000000000
temperature,interval=60,node=rtr.ipiu.ilight.net,name=temp1 temp=140i 1409670791000000000
temperature,interval=60,node=rtr.ipiu.ilight.net,name=temp1 temp=130i 1409670731000000000
temperature,interval=60,node=rtr.ipiu.ilight.net,name=temp1 temp=150i 1409670851000000000";

my $expected_datapoint10 = {
    'time' => 1409670671,
    'values' => {
        'temp' => '100'
    },
    'type' => 'temperature',
    'interval' => '60',
    'meta' => {
        'name' => 'temp1',
        'node' => 'rtr.ipiu.ilight.net',
        'interval' => '60'
    }
};
my $data1 = $influx->parse($input1);
ok(@$data1 == 4, "Got expected data count");

cmp_deeply($data1->[0], $expected_datapoint10, "Got expected data");
