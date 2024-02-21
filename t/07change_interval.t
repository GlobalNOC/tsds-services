use strict;
use warnings;

use Test::More tests => 18;

use GRNOC::Config;
use GRNOC::TSDS::Writer;

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

my $mongo_host = $config->get( '/config/mongo/@host' );
my $mongo_port = $config->get( '/config/mongo/@port' );
my $mongo_uri = "mongodb://$mongo_host:$mongo_port";

my $rabbit_host = $config->get( '/config/rabbit/@host' );
my $rabbit_port = $config->get( '/config/rabbit/@port' );
my $rabbit_queue = $config->get( '/config/rabbit/@queue' );

my $unit_test_db = $config->get( '/config/unit-test-database' );

# connect to rabbit
my $rabbit = Net::AMQP::RabbitMQ->new();

$rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );
$rabbit->channel_open( 1 );

# connect to mongo
my $user = $config->get( "/config/mongo/readonly" );
my $mongo = MongoDB::MongoClient->new(
    host     => $mongo_uri,
    username => $user->{'user'},
    password => $user->{'password'}
);

my $database = $mongo->get_database( $unit_test_db );

# create data type object out of the database
my $data_type = GRNOC::TSDS::DataType->new( name => $unit_test_db,
                                            database => $database );

my $time = 1424131200;
my $val = 0;

# start out with 15 minute interval
my $interval = 60 * 15;

# send some interface data with a 15 minute interval, which will have a one dimensional doc structure
my $data = [];

for my $i ( 1 .. 1000 ) {

    push( @$data, get_message() );
}

$rabbit->publish( 1, $rabbit_queue, $json->encode( $data ), {'exchange' => ''} );

# setup timer to stop writer
$SIG{'ALRM'} = sub {

    $writer->stop();
};

# give writer some time to process docs
alarm( 5 );

# start up a temporary tsds receiver that will read these messages off our queue
diag( "starting writer" );
$writer->start();

diag( "waiting for workers to stop" );
sleep( 10 );

# make sure two documents were created
my $collection = $database->get_collection( 'data' );
my $query1 = {'identifier' => "d0ff413d4228455797d2a7087136ff1f202e1d737b4ad0f89d71d7c8702f46a3"};
my $doc_count = $collection->count_documents( $query1 );
is( $doc_count, 2, "2 docs created" );
my $cursor = $collection->find( $query1 );

my $doc1 = $cursor->next();
my $doc2 = $cursor->next();

is( $doc1->{'start'}, 1423800000, 'start' );
is( $doc1->{'end'}, 1424700000, 'end' );
is( $doc1->{'interval'}, $interval, 'interval' );

is( $doc2->{'start'}, 1424700000, 'start' );
is( $doc2->{'end'}, 1425600000, 'end' );
is( $doc2->{'interval'}, $interval, 'interval' );

# change interval from 900 to 30 seconds
$interval = 30;

# reconnect to rabbit.. why?  im not sure.. but it wont work otherwise
$rabbit = Net::AMQP::RabbitMQ->new();

$rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );
$rabbit->channel_open( 1 );

# publish some more data at this new interval
$data = [get_message(), get_message()];

$rabbit->publish( 1, $rabbit_queue, $json->encode( $data ), {'exchange' => ''} );

# give writer some time to process docs
alarm( 5 );

# start up a temporary tsds receiver that will read these messages off our queue
diag( "starting writer" );
$writer->start();

diag( "waiting for workers to stop" );
sleep( 10 );

# make sure 31 documents now exist for this measurement
my $query2 = {'identifier' => "d0ff413d4228455797d2a7087136ff1f202e1d737b4ad0f89d71d7c8702f46a3"};
$doc_count = $collection->count_documents( $query2 );
is( $doc_count, 31, "31 docs exist" );
$cursor = $collection->find( $query2 )->sort( {'start' => 1} );

my @docs = $cursor->all();

# verify first doc has correct attributes
my $doc = shift( @docs );

is( $doc->{'start'}, 1423800000, 'first doc start' );
is( $doc->{'end'}, 1424700000, 'first doc end' );
is( $doc->{'interval'}, 60 * 15, 'first doc interval' );

# verify last doc has correct attributes
$doc = pop( @docs );

is( $doc->{'start'}, 1425570000, 'last doc start' );
is( $doc->{'end'}, 1425600000, 'last doc end' );
is( $doc->{'interval'}, $interval, 'last doc interval' );

# now, switch it to 1 hour interval
$interval = 60 * 60;

# aaaand reconnect to rabbit...
$rabbit = Net::AMQP::RabbitMQ->new();

$rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );
$rabbit->channel_open( 1 );

$data = [{'type' => $unit_test_db,
          'time' => 1425034800,
          'interval' => $interval,
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
                     'intf' => 'mitch0'}}];

$rabbit->publish( 1, $rabbit_queue, $json->encode( $data ), {'exchange' => ''} );

# give writer some time to process docs
alarm( 15 );

# start up a temporary tsds receiver that will read these messages off our queue
diag( "starting writer" );
$writer->start();

diag( "waiting for workers to stop" );
sleep( 10 );

# make sure only a single document exists again
my $query3 = {'identifier' => "d0ff413d4228455797d2a7087136ff1f202e1d737b4ad0f89d71d7c8702f46a3"};
$doc_count = $collection->count_documents( $query3 );
is( $doc_count, 1, "only one doc exists" );
$cursor = $collection->find( $query3 );

$doc = $cursor->next();

is( $doc->{'start'}, 1422000000, 'start' );
is( $doc->{'end'}, 1425600000, 'end' );
is( $doc->{'interval'}, $interval, 'interval' );

sub get_message {

    my $msg = {'type' => $unit_test_db,
               'time' => $time,
               'interval' => $interval,
               'values' => {'outerror' => $val,
                            'input' => $val,
                            'outUcast' => $val,
                            'status' => $val,
                            'inerror' => $val,
                            'output' => $val,
                            'inUcast' => $val,
                            'indiscard' => $val,
                            'outdiscard' => $val},
               'meta' => {'network' => 'MitchNet',
                          'node' => 'rtr.mitch.net',
                          'intf' => 'mitch0'}};

    $time += $interval;
    $val += 10;

    return $msg;
}

