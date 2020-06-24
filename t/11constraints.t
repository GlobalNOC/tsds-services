use strict;
use warnings;

use Test::More tests => 9;

use GRNOC::TSDS::MongoDB;
use GRNOC::TSDS::Constraints;
use GRNOC::TSDS::DataService::Query;

use Data::Dumper;
use FindBin;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $constraints_file = "$FindBin::Bin/conf/constraints.xml";
my $bnf_file = "$FindBin::Bin/../conf/query_language.bnf";

my $constraints = GRNOC::TSDS::Constraints->new( config_file => $constraints_file );

ok( $constraints, "constraints object created" );

my $mongo = GRNOC::TSDS::MongoDB->new( config_file => $config_file, privilege => 'ro' );

ok( $mongo, "mongo connected" );

my $query = $constraints->parse_constraints( database => 'tsdstest' );

# make sure we parsed the correct constraint queries for this network
is_deeply( $query, {'$and' => [{'node' => {'$regex' => '^rtr'}},
			       {'$or' => [{'node' => 'rtr.chic'},
					  {'node' => 'rtr.newy'}]}]}, "parse_constraints()" );


my $allowed_databases = $constraints->get_databases();
is(@$allowed_databases, 2, "got 2 allowed databases");

my $collection = $mongo->get_collection( 'tsdstest', 'measurements' );

my $num_measurements = $collection->count( $query );
is( $num_measurements, 20, "20 measurements" );


my $query = GRNOC::TSDS::DataService::Query->new( config_file      => $config_file,
                                                  bnf_file         => $bnf_file,
						  constraints_file => $constraints_file );

ok($query, "built dataaservice query object");

# Make sure our allowed database works. Since the tsdstest database is in the constraints
# file it is allowed
my $result = $query->run_query( query =>'get average(values.output) between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" ');
ok($result && ! $query->error(), "got response from allowed databases");

# `tsdstest_two` is not in the constraints file so this query should get rejected
$result = $query->run_query( query =>'get average(values.output) between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest_two where intf = "ge-0/0/0" ');
ok(! defined $result && $query->error() =~ m/Not permitted to run/, "got reject from not allowed database");

# And just to be clear, this is different from a non existing database
$result = $query->run_query( query =>'get average(values.output) between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from notexists where intf = "ge-0/0/0" ');
ok(! defined $result && $query->error() =~ m/Unknown database/, "got reject from unknown database");



