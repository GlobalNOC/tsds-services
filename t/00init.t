use strict;
use warnings;

use Test::More tests => 9;

use GRNOC::Config;
use GRNOC::TSDS::MongoDB;
use GRNOC::TSDS::Install;

use MongoDB;
use Net::AMQP::RabbitMQ;
use Cache::Memcached::Fast;
use Redis;
use Tie::IxHash;

use FindBin;
use Data::Dumper;

# parse testing config file
my $config_file = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";
my $constraints_file = "$FindBin::Bin/conf/constraints.xml";

# intialize logging and our config object
GRNOC::Log->new(config => $logging_file);

my $exists = -e $config_file;

# make sure it exists
if ( !$exists ) {
  BAIL_OUT( 'no config.xml file found in t/conf, please copy config.xml.example and modify as needed' );
}

$exists = -e $constraints_file;

# make sure it exists
if ( !$exists ) {
  BAIL_OUT( 'no constraints.xml file found in t/conf, please copy constraints.xml.example and modify as needed' );
}

my $config = GRNOC::Config->new( config_file => $config_file,
                                 force_array => 0 );

my $memcache_host = $config->get( '/config/memcache/@host' );
my $memcache_port = $config->get( '/config/memcache/@port' );

my $memcache = Cache::Memcached::Fast->new( {'servers' => [{'address' => "$memcache_host:$memcache_port", 'weight' => 1}]} );

# flush everything out of cache
$memcache->flush_all();

my $redis_host = $config->get( '/config/redis/@host' );
my $redis_port = $config->get( '/config/redis/@port' );

my $redis = Redis->new( server => "$redis_host:$redis_port" );

# flush all locks
$redis->flushall();

my $unit_test_db = $config->get( '/config/unit-test-database' );

my $mongo = GRNOC::TSDS::MongoDB->new( config_file => $config_file, privilege => 'root' );

if ( !defined( $mongo ) ) {
  BAIL_OUT( "Can't connect to mongo." );
}

my $database = $mongo->get_database( $unit_test_db );

# delete the db since we are going to recreate it (if it exists)
$database->drop() if $database;

my $database_dir = "$FindBin::Bin/conf/databases/";
my $tsds_install = GRNOC::TSDS::Install->new(
  testing_mode => 1,
  install_dir => $database_dir,
  config_file => $config_file );

my $installed = $tsds_install->install();
diag( 'GRNOC::TSDS::Install::install error: ' . $tsds_install->error) if defined($tsds_install->error);
ok($installed, "Install Succeeded");

# ISSUE=12363 verify sharding
my $sharded_collections = [ 'data', 'data_300', 'data_3600', 'data_86400', 'event', 'measurements' ];

foreach my $collection_name ( @$sharded_collections ) {

    my $output = $mongo->_execute_mongo( "db.getSiblingDB( \"$unit_test_db\" ).getCollection( \"$collection_name\" ).stats()" );
    my $sharded = $output->{'sharded'};

    ok( $sharded, "$collection_name is sharded" );
}

# make sure temp database/collection is sharded too
my $output = $mongo->_execute_mongo( "db.getSiblingDB( \"__tsds_temp_space\" ).getCollection( \"__workspace\" ).stats()" );
my $sharded = $output->{'sharded'};

ok( $sharded, "__tsds_temp_space is sharded" );

# initialize rabbit queue
my $rabbit_host = $config->get( '/config/rabbit/@host' );
my $rabbit_port = $config->get( '/config/rabbit/@port' );
my $rabbit_queue = $config->get( '/config/rabbit/@queue' );

my $rabbit = Net::AMQP::RabbitMQ->new();

$rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );
$rabbit->channel_open( 1 );

# create a new queue if one doesn't already exist
$rabbit->queue_declare( 1, $rabbit_queue, {'auto_delete' => 0} );

# remove all prior messages from queue, if any
$rabbit->purge( 1, $rabbit_queue );

ok( 1, "initialized mongo and rabbit" );
