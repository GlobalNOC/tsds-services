use strict;
use warnings;

use Test::More tests => 21;

use GRNOC::Config;
use GRNOC::TSDS::DataService::MetaData;

use GRNOC::Log;

use JSON::XS;
use MongoDB;
use FindBin;
use Data::Dumper;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";

my $config    = GRNOC::Config->new(config_file => $config_file, force_array => 0);
my $testdb    = $config->get( '/config/unit-test-database' );

GRNOC::Log->new(config => $logging_file);

my $meta_ds = GRNOC::TSDS::DataService::MetaData->new(config_file => $config_file);

# We're going to test some functions in the MetaData object

# get_measurement_types
my $types = $meta_ds->get_measurement_types(show_measurement_count => 1);
ok(defined $types && @$types > 0, "fetched all measurement types");
my $found;
foreach my $type (@$types){
    next unless ($type->{'name'} eq $testdb);
    $found = $type;
}
ok(defined $found, "found $testdb measurement");
is($found->{'measurement_count'}, 31, "got all active measurements in $testdb" );


# get_meta_fields
my $meta_fields = $meta_ds->get_meta_fields(measurement_type => $testdb);
ok(defined $meta_fields && @$meta_fields == 6, "got all meta fields for $testdb");


# get_measurement_type_schemas
my $measurement_type_schema = $meta_ds->get_measurement_type_schemas(measurement_type => [$testdb])->{$testdb};
ok(defined $measurement_type_schema, "got measurement schema");
is(@{$measurement_type_schema->{'value'}{'ordinal'}}, 2, "got both ordinal values");



# get_measurement_type_values
my $values = $meta_ds->get_measurement_type_values(measurement_type => $testdb);
ok($values, "got values");
is(@$values, 11, "got all 11 value types for $testdb");
ok(defined $values->[0]{'name'}, "name field");
ok(defined $values->[0]{'description'}, "description field");
ok(defined $values->[0]{'units'}, "units field");


# get_measurement_type_ordinal_values
my $ord_values = $meta_ds->get_measurement_type_ordinal_values(measurement_type => $testdb);
ok($ord_values, "got ordinal values");
is(@$ord_values, 2, "got both ordinal values");


# get_meta_field_values
my $meta_values = $meta_ds->get_meta_field_values(measurement_type => $testdb,
						  meta_field       => 'intf',
						  limit            => 5,
						  offset           => 0);
ok($meta_values, "got meta values");
is(@$meta_values, 5, "got all 5 meta values");
ok(defined $meta_values->[0]{'value'}, "value field");



# get_distinct_meta_field_values
my $distinct_meta = $meta_ds->get_distinct_meta_field_values(measurement_type => $testdb,
							     meta_field       => 'node',
							     limit            => 2,
							     offset           => 0);
ok($distinct_meta, "got distinct_meta");
is(@$distinct_meta, 2, "got 2 distinct meta fields");
ok(defined $distinct_meta->[0]{'value'}, "value field");

# make sure running on complex fields works
$distinct_meta = $meta_ds->get_distinct_meta_field_values(measurement_type => $testdb,
							  meta_field       => 'circuit.name',
							  limit            => 2,
							  offset           => 0);

ok($distinct_meta, "got distinct_meta");
is(@$distinct_meta, 2, "got 2 distinct meta fields");
