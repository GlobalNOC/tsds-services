use strict;
use warnings;

use Test::More tests => 17;

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
is($res->[0]->{'values.output'}[-1][0], 7190, "last timestamp of base data is correct");


$res = $query->run_query(query => 'get values.output, node, intf, description between(0, 50) from tsdstest where intf = "ge-0/0/0" and node = "rtr.chic" and description = "test123"');
ok(defined $res, "got query response");
is($res->[0]->{'values.output'}[-1][0], 40, "last timestamp of time bound metadata data is correct");


#
# We're going to temporarily truncate an interface record so that we can do some "group by" queries
# by various fields, including __timestamp
# 
$metadata = {
    node        => "rtr.chic",
    intf        => "ge-0/0/0",
    type        => 'tsdstest',
    start       => 100,
    end         => 500,    
    description => undef
};


$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(defined $res, "got positive response back");


# because we're grouping by first node and ordering by intf, we should guarantee to hit ge-0/0/0,
# which we just truncated to time 500 before, so we should only see those data points
$res = $query->run_query(query => 'get values.output, node, intf between(0, 2000) by node first(intf) from tsdstest where node = "rtr.chic" ordered by intf ');
ok(defined $res, "got query response");
is(@$res, 1, "got 1 result back");
is(@{$res->[0]{'values.output'}}, 50, "got 50 datapoints, truncated to metadata correctly");
is($res->[0]{'values.output'}->[-1][0], 490, "got correct last timepoint");


# if we add the __timestamp however, it should fill in data from a DIFFERENT interface after
# ge-0/0/0 one since it's treating the first(intf) as being first within a specified timestamp
$res = $query->run_query(query => 'get values.output, node, intf between(0, 2000) by node first(intf), __timestamp from tsdstest where node = "rtr.chic" ordered by intf ');

ok(defined $res, "got query response");
is(@$res, 1, "got 1 result back");
is(@{$res->[0]{'values.output'}}, 200, "got 200 datapoints, __timestamp filled in extra");
is($res->[0]{'values.output'}->[-1][0], 1990, "got correct last timepoint");

is($res->[0]{'values.output'}->[49][1], 103730, "last value from ge-0/0/0 is 103730");
is($res->[0]{'values.output'}->[50][1], 146931, "first value from interface1 is 146931");
