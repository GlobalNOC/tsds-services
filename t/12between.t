use strict;
use warnings;

use Test::More tests => 17;
use GRNOC::Config;
use GRNOC::Log;
use GRNOC::TSDS::DataService::Query;
use Data::Dumper;
use FindBin;


my $config_file  = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";
my $bnf_file = "$FindBin::Bin/../conf/query_language.bnf";

GRNOC::Log->new( config => $logging_file );
my $query = GRNOC::TSDS::DataService::Query->new( config_file => $config_file,
                                                  bnf_file => $bnf_file );
ok($query, "query data service connected");


# Test various ways of specifying between() to ensure we have the correct time ranges

# First baseline using the original method of specifying the human friendly timespec
my $arr = $query->run_query( query =>'get values.output, average(values.output) as avg between ("01/01/1970 00:00:00 UTC","01/01/1970 01:00:00 UTC") from tsdstest where intf = "ge-0/0/0" ');
ok($arr, "got initial result");
is($arr->[0]{'values.output'}[0][0], 0, "first timestamp");
is($arr->[0]{'values.output'}[-1][0], 3590, "last timestamp");
is($arr->[0]{'avg'}, 180.5, "average");


# Now use "now" which is kind of dumb since it's a unit test
# but we can ask for time 0 until now which should get the results
$arr = $query->run_query( query =>'get values.output, average(values.output) as avg between ("01/01/1970 00:00:00 UTC",now) from tsdstest where intf = "ge-0/0/0" ');
is($arr->[0]{'values.output'}[0][0], 0, "first timestamp");
is($arr->[0]{'values.output'}[-1][0], 89990, "last timestamp");
is($arr->[0]{'avg'}, 4320.5, "average");


# "now" with a relative modifier, will ultimately be the same as above
$arr = $query->run_query( query =>'get values.output, average(values.output) as avg between ("01/01/1970 00:00:00 UTC",now-1m) from tsdstest where intf = "ge-0/0/0" ');
is($arr->[0]{'values.output'}[0][0], 0, "first timestamp");
is($arr->[0]{'values.output'}[-1][0], 89990, "last timestamp");
is($arr->[0]{'avg'}, 4320.5, "average");


# epoch timestamps
$arr = $query->run_query( query =>'get values.output, average(values.output) as avg between (0,7200) from tsdstest where intf = "ge-0/0/0" ');
is($arr->[0]{'values.output'}[0][0], 0, "first timestamp");
is($arr->[0]{'values.output'}[-1][0], 7190, "last timestamp");
is($arr->[0]{'avg'}, 360.5, "average");


# intermingled
$arr = $query->run_query( query =>'get values.output, average(values.output) as avg between (0,"01/01/1970 02:00:00 UTC") from tsdstest where intf = "ge-0/0/0" ');
is($arr->[0]{'values.output'}[0][0], 0, "first timestamp");
is($arr->[0]{'values.output'}[-1][0], 7190, "last timestamp");
is($arr->[0]{'avg'}, 360.5, "average");
