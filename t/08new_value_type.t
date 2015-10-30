use strict;
use warnings;

use Test::More tests => 11;

use GRNOC::Config;
use GRNOC::TSDS::Writer;
use GRNOC::TSDS::DataDocument;
use GRNOC::TSDS::DataType;

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

# send a message which contains new value types in a brand new doc
my $data = [{'type' => $unit_test_db,
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
                        'intf' => 'mitch1'}}];

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

# make sure all metadata value types exist now
# create data type object out of the database
my $data_type = GRNOC::TSDS::DataType->new( name => $unit_test_db,
                                            database => $database );

my $value_types = $data_type->value_types;

ok( defined( $value_types->{'outerror'} ), 'outerror' );
ok( defined( $value_types->{'outUcast'} ), 'outUcast' );
ok( defined( $value_types->{'status'} ), 'status' );
ok( defined( $value_types->{'inerror'} ), 'inerror' );
ok( defined( $value_types->{'inUcast'} ), 'inUcast' );
ok( defined( $value_types->{'indiscard'} ), 'indiscard' );
ok( defined( $value_types->{'outdiscard'} ), 'outdiscard' );

# re-connect to rabbit...
$rabbit = Net::AMQP::RabbitMQ->new();

$rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );
$rabbit->channel_open( 1 );

# send a message which contains a new value types but for an existing doc
$data = [{'type' => $unit_test_db,
          'time' => 1424131500,
          'interval' => 300,
          'values' => {'newtypelol1' => 0},
          'meta' => {'network' => 'MitchNet',
                     'node' => 'rtr.mitch.net',
                     'intf' => 'mitch1'}},
         {'type' => $unit_test_db,
          'time' => 1424131800,
          'interval' => 300,
          'values' => {'newtypelol2' => 1},
          'meta' => {'network' => 'MitchNet',
                     'node' => 'rtr.mitch.net',
                     'intf' => 'mitch1'}}];

$rabbit->publish( 1, $rabbit_queue, $json->encode( $data ), {'exchange' => ''} );

# give writer some time to process docs
alarm( 5 );

# start up a temporary tsds receiver that will read these messages off our queue
diag( "starting writer" );
$writer->start();

diag( "waiting for workers to stop" );
sleep( 10 );

# make sure new type exists in metadata
$data_type = GRNOC::TSDS::DataType->new( name => $unit_test_db,
                                         database => $database );

$value_types = $data_type->value_types;

ok( defined( $value_types->{'newtypelol1'} ), 'newtypelol1' );
ok( defined( $value_types->{'newtypelol2'} ), 'newtypelol2' );

# get the document that should be created
my $doc = GRNOC::TSDS::DataDocument->new( data_type => $data_type,
                                          measurement_identifier => "bdbd5926b074ab1890b7060f966a9df1cc41d0fd6210baa76f2aec7e51537b4a",
                                          start => 1424100000,
                                          end => 1424400000 );

$doc->fetch();

$value_types = $doc->value_types;

ok( defined( $value_types->{'newtypelol1'} ), 'newtypelol1' );
ok( defined( $value_types->{'newtypelol2'} ), 'newtypelol2' );
