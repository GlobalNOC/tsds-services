use strict;
use warnings;

use Test::More tests => 10;

use GRNOC::Config;
use GRNOC::TSDS::Constants;
use GRNOC::TSDS::Writer;

use JSON::XS;
use MongoDB;
use Net::AMQP::RabbitMQ;
use Net::RabbitMQ::Management::API;
use POSIX qw( ceil );
use FindBin;
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

# blow out the channel in case other things were lurking around from a previous run
$rabbit->purge(1, $rabbit_queue);


# connect to mongo to ensure data before/after
my $user = $config->get( "/config/mongo/readwrite" );
my $mongo = MongoDB::MongoClient->new(
    host     => $mongo_uri,
    username => $user->{'user'},
    password => $user->{'password'}
);

my $database = $mongo->get_database( $unit_test_db );
my $collection = $database->get_collection( 'measurements' );

# clear out anything prior and grab our test intfs
$collection->update_many({}, {'$unset' => {'tags' => 1, 'description' => 1}});

my @active = $collection->find({"node" => "rtr.seat", "end" => undef})->all();
is(@active, 2, "currently 2 active records for rtr.seat");

my $measurements = {};
foreach my $doc (@active){
    is($doc->{'tags'}, undef, "doc doesn't have tags set");
    is($doc->{'description'}, undef, "doc doesn't have description set");
    $measurements->{$doc->{'node'}}{$doc->{'intf'}} = 1;
}


# handle all measurements
foreach my $node (keys %$measurements){

    foreach my $intf (keys %{$measurements->{$node}}){
        my $time = 0;

	# We'll be setting network and description metadata fields.
	# These shouldn't exist yet
	my $message = {
	    'type' => $unit_test_db . ".metadata",
	    'time' => $time,
	    'meta' => {
		'node' => $node,
		'intf' => $intf,
		'tags' => ["Tag 1 for $intf", "Tag 2 for $intf"],
		"description" => "Description for $node $intf"
	    }
	};

	_send([$message]);

	# We're also going to push a garbage message to make sure it handles that okay
	$message = {
	    'type' => $unit_test_db . ".metadata",
	    'time' => $time,
	    'meta' => {
		'node' => $node,
		'intf' => $intf,
		'INVALID_FIELD' => 'This should not be accepted'
	    }	
	};

	_send([$message]);        
    }
}

# setup rabbitmq management to check queue depth
my $rabbit_management = Net::RabbitMQ::Management::API->new(url => "http://" . $rabbit_host . ":15672/api");

# will take longer to run under devel cover
my $max_time = $INC{'Devel/Cover.pm'} ? 800 : 200;

my $start = time();

# setup timer to stop writer
$SIG{'ALRM'} = sub {

    my $queue_size = -1;
    eval {
	$queue_size = $rabbit_management->get_queue( name => $rabbit_queue,	
						     vhost => '%2f' )->content()->{'messages'};
	diag("queue size is $queue_size");
    };
    if ( $@ ) {
	warn("Error getting queue info: $@" );
    }

    if ($queue_size == 0 || time() - $start >= $max_time){
	$writer->stop();
    }
    else {
	alarm( 10 );
    }
};

# kick off our watcher
alarm( 10 );

# start up a temporary tsds receiver that will read messages off our queue
diag( "starting writer" );
$writer->start();

# the alarm will stop the writer and we just need to wait for child processes to exit
diag( "waiting for workers to stop" );
sleep( 10 );


# Now that we have written the messages, let's make sure our metadata docs were updated accordingly
my @active = $collection->find({"node" => "rtr.seat", "end" => undef})->all();
is(@active, 2, "currently 2 active records for rtr.seat");

my $measurements = {};
foreach my $doc (@active){
    my $node = $doc->{'node'};
    my $intf = $doc->{'intf'};
    is($doc->{'description'}, "Description for $node $intf", "doc now has description set");
    ok(ref($doc->{'tags'}) eq 'ARRAY' && @{$doc->{'tags'}} == 2, "doc now has tags set");
}


### helper methods ###

sub _send {

    my ( $messages ) = @_;

    $rabbit->publish( 1, $rabbit_queue, $json->encode( $messages ), {'exchange' => ''} );
}
