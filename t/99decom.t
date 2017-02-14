use strict;
use warnings;

use Test::More tests => 9;

use GRNOC::Log;
use GRNOC::TSDS::MeasurementDecommer;

use MongoDB;

use Data::Dumper;
use FindBin;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";

GRNOC::Log->new(config => $logging_file);

my $config = GRNOC::Config->new( config_file => $config_file,
                                 force_array => 0 );

my $mongo_host = $config->get('/config/mongo/@host');
my $mongo_port = $config->get('/config/mongo/@port');
my $user       = $config->get('/config/mongo/root');

# add some dummy test data
my $mongo = MongoDB::MongoClient->new(
    host     => "mongodb://$mongo_host:$mongo_port",
    username => $user->{'user'},
    password => $user->{'password'}
);

my $unit_test_db = $config->get( '/config/unit-test-database' );

my $measurements = $mongo->get_database($unit_test_db)->get_collection("measurements");
my $data         = $mongo->get_database($unit_test_db)->get_collection("data");

my $IDENTIFIER = "100decom.t";

# clear out anything left in test db from a previous test here
$measurements->remove({"identifier" => $IDENTIFIER});
$data->remove({"identifier" => $IDENTIFIER});


my $decommer = GRNOC::TSDS::MeasurementDecommer->new( config_file => $config_file,
						      database    => $unit_test_db );
ok( defined( $decommer ), "decommer object created" );

# run decommer once before we do anything else to make sure we're in a known state
$decommer->decom_metadata();


# our placeholder records for later
my $measurement_record = {
    "identifier" => $IDENTIFIER,
    "start" => 10,
    "end" => undef,
    "intf" => "interface-test-100decom.t",
    "node" => "node-test-100decom.t"
};

my $data_record = {  
    "identifier" => $IDENTIFIER,
    "start"      => 10,
    "end"        => 10000,
    "interval"   => 10,
    "values"     => {"input" => [] } # this isn't accurate as to what the data structure looks like but doesn't matter for this test
};

# add measurement record
$measurements->insert_one($measurement_record);

# add data record last inserted $now, we're looking at the _id field
# to get a rough "created at" date
$data_record->{"_id"} = MongoDB::OID->new(value => sprintf("%X", time()));
$data->insert_one($data_record);

my $results = $decommer->decom_metadata();

ok(defined $results, "got results back from decom metadoata");
is(keys %$results, 1, "got 1 database back in results");
is($results->{$unit_test_db}, 0, "got 0 decom results");

my $updated_doc = $measurements->find_one({identifier => $IDENTIFIER});
is($updated_doc->{'end'}, undef, "measurement doc left alone correctly");

# clear out docs
$data->remove({"identifier" => $IDENTIFIER});

# add a data record where the last inserted was 2 days ago, ie older than the expire_after 
# for this measurement type
$data_record->{"_id"} = MongoDB::OID->new(value => sprintf("%X", time() - (86400 * 2)));
$data->insert_one($data_record);

# re-run decommer
$results = $decommer->decom_metadata();

ok(defined $results, "got results back from decom metadoata");
is(keys %$results, 1, "got 1 database back in results");
is($results->{$unit_test_db}, 1, "got 1 decom results");

$updated_doc = $measurements->find_one({identifier => $IDENTIFIER});
is($updated_doc->{'end'}, 10000, "measurement doc end updated correctly");
