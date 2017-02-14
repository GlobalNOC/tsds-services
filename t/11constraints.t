use strict;
use warnings;

use Test::More tests => 4;

use GRNOC::TSDS::MongoDB;
use GRNOC::TSDS::Constraints;

use Data::Dumper;
use FindBin;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $constraints_file = "$FindBin::Bin/conf/constraints.xml";

my $constraints = GRNOC::TSDS::Constraints->new( config_file => $constraints_file );

ok( $constraints, "constraints object created" );

my $mongo = GRNOC::TSDS::MongoDB->new( config_file => $config_file, privilege => 'ro' );

ok( $mongo, "mongo connected" );

my $query = $constraints->parse_constraints( database => 'tsdstest' );

# make sure we parsed the correct constraint queries for this network
is_deeply( $query, {'$and' => [{'node' => {'$regex' => '^rtr'}},
			       {'$or' => [{'node' => 'rtr.chic'},
					  {'node' => 'rtr.newy'}]}]}, "parse_constraints()" );

my $collection = $mongo->get_collection( 'tsdstest', 'measurements' );

my $num_measurements = $collection->count( $query );
is( $num_measurements, 20, "20 measurements" );
