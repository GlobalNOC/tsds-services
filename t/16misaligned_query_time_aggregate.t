use strict;
use warnings;

use Test::More tests => 15;

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

my $query   = GRNOC::TSDS::DataService::Query->new(config_file => $config_file,
                                                   bnf_file    => $bnf_file);


# We have a 5 minute aggregate from the bootstrapping, so we're going to first query hires
# and verify that that works as normal
my $res = $query->run_query(query => 'get values.output, node, intf, description between(0, 7200) from tsdstest where intf = "ge-0/0/0" and node = "rtr.chic"');
ok(defined $res, "got query response");
is($res->[0]->{'values.output'}[0][0], 0,  "first timestamp of hi-res data is correct");
is($res->[0]->{'values.output'}[-1][0], 7190, "last timestamp of hi-res data is correct");
is($query->actual_start(), 0, "actual query start time reported as 0");
is($query->actual_end(), 7200, "actual query end time reported as 7200");


# Then we can query directly on the 300s interval and get a result like normal
# 3000 % 300 = 0, so end should be 7200
# 300 % 0 = 0, so start should be 0
# Also verify that the meta fields are there
$res = $query->run_query(query => 'get aggregate(values.output, 300, average) as values.output, node, intf, description between(300, 3000) from tsdstest where intf = "ge-0/0/0" and node = "rtr.chic"');
ok(defined $res, "got query response");
is($res->[0]->{'values.output'}[0][0], 300,  "first timestamp of aligned aggregate data is correct");
is($res->[0]->{'values.output'}[-1][0], 2700, "last timestamp of aligned aggregate data is correct");


is($query->actual_start(), 300, "actual query start time reported as 0");
is($query->actual_end(), 3000, "actual query end time reported as 3000");


# Then we will try to query with the start and end times misaligned from 300s but still
# asking for 300s data. This should return a query where the start is floored and the end
# is ceiled to 300s to make sure it encompasses all the data.
# Also verify that the meta fields are there
$res = $query->run_query(query => 'get aggregate(values.output, 300, average) as values.output, node, intf, description between(7, 641) by intf, node from tsdstest where intf = "ge-0/0/0" and node = "rtr.chic"');

ok(defined $res, "got query response");
is($res->[0]->{'values.output'}[0][0], 0, "first timestamp of misaligned aggregate data query is correct");
is($res->[0]->{'values.output'}[-1][0], 600, "last timestamp of midsaligned aggregate data quer is correcT");

is($query->actual_start(), 0, "got floored start time of 0");
is($query->actual_end(), 900, "got ceiled start time of 900");
