use strict;
use warnings;

use Test::More tests => 2;

use GRNOC::Config;
use GRNOC::TSDS::Writer;
use GRNOC::TSDS::DataDocument;
use GRNOC::TSDS::DataType;

use Net::RabbitMQ::Management::API;
use Net::AMQP::RabbitMQ;
use JSON::XS;

use FindBin;
use Data::Dumper;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";

my $writer = GRNOC::TSDS::Writer->new( config_file => $config_file,
				       logging_file => $logging_file,
                                       daemonize => 0 );

my $config = GRNOC::Config->new( config_file => $config_file,
                                 force_array => 0 );

my $json = JSON::XS->new();

my $rabbit_host = $config->get( '/config/rabbit/@host' );
my $rabbit_port = $config->get( '/config/rabbit/@port' );
my $rabbit_queue = $config->get( '/config/rabbit/@queue' );

my $unit_test_db = $config->get( '/config/unit-test-database' );

# connect to rabbit
my $rabbit = Net::AMQP::RabbitMQ->new();

$rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );
$rabbit->channel_open( 1 );

# ISSUE=10944 send non-array data
my $bad_data1 = {'type' => $unit_test_db,
		 'time' => 1424131200,
		 'interval' => 300,
		 'values' => {'outerror' => 0,
			      'input' => 1,
			      'outUcast' => 2,
			      'status' => 3,
			      'inerror' => 4,
			      'output' => 5,
			      'inUcast' => 6,
			      'indiscard' => 7,
			      'outdiscard' => 8},
		 'meta' => {'network' => 'MitchNet',
			    'node' => 'rtr.mitch.net',
			    'intf' => 'mitch1'}};

my $bad_data2 = {};
my $bad_data3 = [];
my $bad_data4 = [undef];
my $bad_data5 = ["lolwut"];
my $bad_data6 = [{}];
my $bad_data7 = [{'meow' => 'mix'}];
my $bad_data8 = "im_not_json_lol";

# ISSUE=755:160 non-numeric values values
my $bad_data9 = [
   {"type" => $unit_test_db,
    "time" => int(time()),
    "interval" => 300,
    "values" => {
             "input" => "whoops not a number",
             "output" => "nan"
    },
    "meta" => { "network" => "MitchNet",
                "node"    => "rtr.mitch.net",
                "intf"    => "mitch1"
    }
   }
];

$rabbit->publish( 1, $rabbit_queue, $json->encode( $bad_data1 ), {'exchange' => ''} );
$rabbit->publish( 1, $rabbit_queue, $json->encode( $bad_data2 ), {'exchange' => ''} );
$rabbit->publish( 1, $rabbit_queue, $json->encode( $bad_data3 ), {'exchange' => ''} );
$rabbit->publish( 1, $rabbit_queue, $json->encode( $bad_data4 ), {'exchange' => ''} );
$rabbit->publish( 1, $rabbit_queue, $json->encode( $bad_data5 ), {'exchange' => ''} );
$rabbit->publish( 1, $rabbit_queue, $json->encode( $bad_data6 ), {'exchange' => ''} );
$rabbit->publish( 1, $rabbit_queue, $json->encode( $bad_data7 ), {'exchange' => ''} );
$rabbit->publish( 1, $rabbit_queue, $bad_data8, {'exchange' => ''} );
$rabbit->publish( 1, $rabbit_queue, $json->encode( $bad_data9 ), {'exchange' => ''} );

my $num;

# setup timer to stop writer
$SIG{'ALRM'} = sub {

    $num = $writer->stop();
};

# give writer some time to process docs
alarm( 10 );

# start up a temporary tsds receiver that will read these messages off our queue
diag( "starting writer" );
$writer->start();

diag( "waiting for workers to stop" );
sleep( 10 );

ok( $num > 0, "stopped children" );


# Verify that all messages were handled and queue is now empty, 
# ie no messages got requeued so all errors gracefully handled

# create rabbit management object
my $rabbit_management = Net::RabbitMQ::Management::API->new( url => "http://localhost:15672/api" );
my $queue_size = $rabbit_management->get_queue( name  => $rabbit_queue,
                                                vhost => '%2f' )->content()->{'messages'};

is($queue_size, 0, "all messages consumed ok");
