use strict;
use warnings;

use Test::More tests => 2;

use GRNOC::TSDS::Aggregate;
use GRNOC::Config;

use Data::Dumper;
use FindBin;

use constant TIME_START => 0;
use constant TIME_END => 86400;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";

my $config = GRNOC::Config->new( config_file => $config_file,
                                 force_array => 0 );

my $unit_test_db = $config->get( '/config/unit-test-database' );

my $agent = GRNOC::TSDS::Aggregate->new( config_file => $config_file,
                                         logging_file => $logging_file,
                                         lock_dir => '/tmp',
                                         database => $unit_test_db,
                                         start => TIME_START,
                                         end => TIME_END,
                                         quiet => 1 );

ok( defined( $agent ), "agent object created" );

# aggregate all the things
my $ret = $agent->aggregate_data();

ok( $ret, "aggregated" );
