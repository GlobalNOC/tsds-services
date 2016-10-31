use strict;
use warnings;

use Test::More tests => 6;

use GRNOC::Config;
use GRNOC::TSDS::DataService::MetaData;
use GRNOC::TSDS::DataService::Query;

use GRNOC::Log;

use JSON::XS;
use MongoDB;
use FindBin;
use Data::Dumper;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";
my $bnf_file = "$FindBin::Bin/../conf/query_language.bnf";

GRNOC::Log->new(config => $logging_file);

my $meta_ds = GRNOC::TSDS::DataService::MetaData->new(config_file => $config_file);
my $query   = GRNOC::TSDS::DataService::Query->new(config_file => $config_file,
                                                   bnf_file    => $bnf_file);

# This is one of the original interfaces from 01bootstrap.t, it will have
# only a single measurement entry and a lot of associated data for a day
# We're going to insert a record into the middle of that data and then
# do a query that should only touch part of it to ensure we hit the
# limits right
my $metadata = {
    node        => "rtr.chic",
    intf        => "ge-0/0/0",
    type        => 'tsdstest',
    start       => 0,
    end         => 100,
    description => "test123"  # this is new
};

my $res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(defined $res, "got positive response back");

$metadata->{'description'} = undef;
$metadata->{'start'} = 100;
$metadata->{'end'} = undef;
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(defined $res, "got positive response back");


$res = $query->run_query(query => 'get values.output, node, intf, description between(0, 7200) from tsdstest where intf = "ge-0/0/0" and node = "rtr.chic"');
ok(defined $res, "got query response");
is($res->[0]->{'values.output'}[-1][0], 7200, "last timestamp of base data is correct");


$res = $query->run_query(query => 'get values.output, node, intf, description between(0, 50) from tsdstest where intf = "ge-0/0/0" and node = "rtr.chic" and description = "test123"');
ok(defined $res, "got query response");
is($res->[0]->{'values.output'}[-1][0], 50, "last timestamp of time bound metadata data is correct");
