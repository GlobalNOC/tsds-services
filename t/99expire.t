use strict;
use warnings;

use Test::More tests => 5;

use GRNOC::TSDS::Expire;

use Data::Dumper;
use FindBin;

use constant TIME_START => 0;
use constant TIME_END => 86_400_000; # 1000 days

my $config_file = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";

my $config = GRNOC::Config->new( config_file => $config_file,
                                 force_array => 0 );

my $unit_test_db = $config->get( '/config/unit-test-database' );

my $agent = GRNOC::TSDS::Expire->new( config_file => $config_file,
                                      logging_file => $logging_file,
                                      database => $unit_test_db,
                                      start => TIME_START,
                                      end => TIME_END );

ok( defined( $agent ), "agent object created" );

my $num_removed;

# expire default/hires retention data
$agent->expire( "default" );
$num_removed = $agent->expire_data();
is( $num_removed, 180, "expired 180 hires docs (9 each for 20 measurements)" );

# expire five minute retention data
$agent->expire( "five_min" );
$num_removed = $agent->expire_data();
is( $num_removed, 0, "expired 0 five minute docs" );

# expire one hour retention data
$agent->expire( "one_hour" );
$num_removed = $agent->expire_data();
is( $num_removed, 0, "expired 0 one hour docs" );

# expire one day retention data
$agent->expire( "one_day" );
$num_removed = $agent->expire_data();
is( $num_removed, 0, "expired 0 one day docs" );
