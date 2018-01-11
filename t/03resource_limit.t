use strict;
use warnings;

use Test::More tests => 11;

use GRNOC::Config;
use GRNOC::Log;
use GRNOC::TSDS::DataService::Query;
use GRNOC::TSDS::DataService::MetaData;

use FindBin;
use Data::Dumper;

GRNOC::Log->new(config => "$FindBin::Bin/../conf/logging.conf");

my $config_file = "$FindBin::Bin/conf/config.xml";
my $bnf_file = "$FindBin::Bin/../conf/query_language.bnf";

# instatiate query ds 
my $query = GRNOC::TSDS::DataService::Query->new( 
    config_file => $config_file,
    bnf_file => $bnf_file
);
ok($query, "query data service connected");

# instatiate admin ds
my $metadata = GRNOC::TSDS::DataService::MetaData->new( 
    config_file => $config_file,
    bnf_file => $bnf_file
);
ok($metadata, "metadata data service connected");

# add a measurement type name
my $measurement_type_response = $metadata->add_measurement_type(
  name => 'measurement_type_name',
  label => 'Checking the measurement type name',
  required_meta_field => ['node']
);
ok($measurement_type_response->[0]{'success'}, "measurement type added");

# set the data_doc_limit
my $res = $metadata->update_measurement_types(
    name => 'tsdstest',
    data_doc_limit => 1
);
ok($res->[0]{'success'}, "data_doc_limit set");

# erify query blocked 
my $arr= $query->run_query( query =>'get average(values.output) between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" ');
ok(!defined($arr), "query blocked");

# remove the data_doc_limit
$res = $metadata->update_measurement_types(
    name => 'tsdstest',
    data_doc_limit => undef
);
ok($res->[0]{'success'}, "data_doc_limit removed");

# Verify query works after data_doc_limit removed
$arr= $query->run_query( query =>'get average(values.output) between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" ');
ok($arr, "query returned");



# set event_limit
$res = $metadata->update_measurement_types(
    name => 'tsdstest',
    event_limit => 40
);
ok($res->[0]{'success'}, "event_limit set");

# verify event query blocked
$arr= $query->run_query(query => "get type, start, end, identifier, text, node between(\"01/01/1970 00:00:00 UTC\", \"01/02/1970 00:00:00 UTC\") from tsdstest.event ordered by start, text");
ok(!defined($arr), "event query blocked");

# remove event_limit
$res = $metadata->update_measurement_types(
    name => 'tsdstest',
    event_limit => undef
);
ok($res->[0]{'success'}, "event_limit removed");

# verify event query 
$arr= $query->run_query(query => "get type, start, end, identifier, text, node between(\"01/01/1970 00:00:00 UTC\", \"01/02/1970 00:00:00 UTC\") from tsdstest.event ordered by start, text");
ok($arr, "event query returned");

