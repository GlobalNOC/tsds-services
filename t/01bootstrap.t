use strict;
use warnings;

use Test::More tests => 2;

use GRNOC::Config;
use GRNOC::TSDS::Constants;
use GRNOC::TSDS::Writer;

use JSON::XS;
use MongoDB;
use Net::AMQP::RabbitMQ;
use POSIX qw( ceil );
use FindBin;

use constant NUM_DATA_POINTS => 8640; # one day
use constant MESSAGE_SIZE => 40;
use constant MAX_VALUE => 10_000_000_000;
use constant INTERVAL => 10;
use constant NUM_EVENTS => 1000;

use Data::Dumper;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";

my $writer = GRNOC::TSDS::Writer->new( config_file => $config_file,
                                       logging_file => $logging_file,
                                       daemonize => 0 );

my $config = GRNOC::Config->new( config_file => $config_file,
                                 force_array => 0 );

my $mongo_host = $config->get( '/config/mongo/@host' );
my $mongo_port = $config->get( '/config/mongo/@port' );
my $mongo_uri = "mongodb://$mongo_host:$mongo_port";

my $rabbit_host = $config->get( '/config/rabbit/@host' );
my $rabbit_port = $config->get( '/config/rabbit/@port' );
my $rabbit_queue = $config->get( '/config/rabbit/@queue' );

my $unit_test_db = $config->get( '/config/unit-test-database' );

my $json = JSON::XS->new();

# connect to rabbit
my $rabbit = Net::AMQP::RabbitMQ->new();

$rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );
$rabbit->channel_open( 1 );

my $measurements = {};

$measurements->{'rtr.chic'}{'ge-0/0/0'} = 1;
$measurements->{'rtr.newy'}{'xe-0/1/0.0'} = 1;
$measurements->{'rtr.chic'}{'interface3'} =1;
$measurements->{'rtr.chic'}{'interface4'} =1;
$measurements->{'rtr.chic'}{'interface5'} =1;
$measurements->{'rtr.chic'}{'interface6'} =1;
$measurements->{'rtr.chic'}{'interface7'} =1;
$measurements->{'rtr.chic'}{'interface8'} =1;
$measurements->{'rtr.chic'}{'interface9'} =1;
$measurements->{'rtr.chic'}{'interface10'} =1;
$measurements->{'rtr.chic'}{'interface11'} =1;
$measurements->{'rtr.newy'}{'interface3'} =1;
$measurements->{'rtr.newy'}{'interface4'} =1;
$measurements->{'rtr.newy'}{'interface5'} =1;
$measurements->{'rtr.newy'}{'interface6'} =1;
$measurements->{'rtr.newy'}{'interface7'} =1;
$measurements->{'rtr.newy'}{'interface8'} =1;
$measurements->{'rtr.newy'}{'interface9'} =1;
$measurements->{'rtr.newy'}{'interface10'} =1;
$measurements->{'rtr.newy'}{'interface11'} =1;

# first, we'll send data to initialize the measurements
my @nodes = keys( %$measurements );

my $num_measurements = 0;

foreach my $node ( @nodes ) {

    my @intfs = keys( %{$measurements->{$node}} );

    foreach my $intf ( @intfs ) {

        $num_measurements++;

        my $message = {'type' => $unit_test_db,
                       'time' => 0,
                       'interval' => INTERVAL,
                       'values' => {'input' => undef,
                                    'output' => undef},
                       'meta' => {'node' => $node,
                                  'intf' => $intf,
                                  'network' => 'Test Network'}};

        _send( [$message] );
    }
}

# handle all measurements

my $i=0;
my $j=0;   

foreach my $node ( @nodes ) {

    my @intfs = keys( %{$measurements->{$node}} );

    foreach my $intf ( @intfs ) {

        my $messages = [];
        my $time = 0;

        for ( 1 .. NUM_DATA_POINTS ) {

            my $input = int(++$i); # deterministic input values
            my $output = int(++$j); # deterministic output values

            my $message = {'type' => $unit_test_db,
                           'time' => $time,
                           'interval' => INTERVAL,
                           'values' => {'input' => $input,
                                        'output' => $output},
                           'meta' => {'node' => $node,
                                      'intf' => $intf,
                                      'network' => 'Test Network'}};

            push( @$messages, $message );
            $time += INTERVAL;

            # send it off to rabbit/mongo
            if ( @$messages == MESSAGE_SIZE ) {

                _send( $messages );

                $messages = [];
            }
        }

        # send any remaining
        if ( @$messages > 0 ) {

            _send( $messages );
        }
    }
}

# send event information
for (my $i = 1; $i <= NUM_EVENTS; $i++){

    my @messages;

    foreach my $node (@nodes){

        my $message = {'type'  => $unit_test_db . '.event',
                       'start' => 3600 * $i,
                       'end'   => 3600 * $i + 350,
                       'identifier' => "$node event $i",
                       'event_type' => 'alarm',
                       'affected' => {'node' => [$node]},
                       'text' => "This is the event at index $i for node $node"};


        push(@messages, $message);
    }

    _send(\@messages);
}

# setup timer to stop writer
$SIG{'ALRM'} = sub {

    $writer->stop();
};

my $sleep = 200;

# sleep longer if running under Devel::Cover
$sleep = 800 if ( $INC{'Devel/Cover.pm'} );

alarm( $sleep );

# start up a temporary tsds receiver that will read messages off our queue
diag( "starting writer" );
$writer->start();

# the alarm will stop the writer and we just need to wait for child processes to exit
diag( "waiting for workers to stop" );
sleep( 10 );

# make sure all expected data documents were created in mongo
my $user = $config->get( "/config/mongo/readonly" );
my $mongo = MongoDB::MongoClient->new(
    host     => $mongo_uri,
    username => $user->{'user'},
    password => $user->{'password'}
);


my $database = $mongo->get_database( $unit_test_db );
my $collection = $database->get_collection( 'data' );

my $total_docs = $num_measurements * ceil( NUM_DATA_POINTS / HIGH_RESOLUTION_DOCUMENT_SIZE );
my $num_docs = $collection->count( {} );
is( $num_docs, $total_docs, "$total_docs data documents created" );

# make sure all expected event documents were created in mongo
$collection = $database->get_collection( 'event' );
$num_docs = $collection->count( {} );

is( $num_docs, 42, "42 event documents created" );

# re-index all alerts for search
diag( "need to index alerts for search, root/sudo required" );
system( 'sudo /usr/bin/indexer tsds_metadata_index && sudo /usr/bin/indexer tsds_metadata_delta_index' );

### helper methods ###

sub _send {

    my ( $messages ) = @_;

    $rabbit->publish( 1, $rabbit_queue, $json->encode( $messages ), {'exchange' => ''} );
}
