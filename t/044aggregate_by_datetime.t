use strict;
use warnings;
use Test::More tests => 36;
use GRNOC::Config;
use GRNOC::Log;
use GRNOC::TSDS::DataService::Query;
use Data::Dumper;
use List::MoreUtils qw(all);
use FindBin;
use DateTime;

my $config_file  = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";
#my $logging_file = "/home/daldoyle/logging-debug.conf";
my $bnf_file = "$FindBin::Bin/../conf/query_language.bnf";

GRNOC::Log->new( config => $logging_file );
my $query = GRNOC::TSDS::DataService::Query->new( config_file => $config_file,
                                                  bnf_file => $bnf_file );


my ($res, $agg_values);

#
## Fairly basic query sanity check first
$res = $query->run_query(query => 'get aggregate(values.output, 3600, average) as agg_values, values.output between(0, 10800) from tsdstest where intf = "interface1" and node = "rtr.seat" ');

is(@$res, 1, "got 1 result");
is(@{$res->[0]{'agg_values'}}, 3, "3 results in aggregate query");
is(@{$res->[0]{'values.output'}}, 18, "18 hi-res results in query");

#
## Now we're going to align to 1 week using epoch seconds, should generate 2 weeks of results
$res = $query->run_query(query => 'get aggregate(values.output, 604800, average) as agg_values between("01/01/1970", "01/15/1970") from tsdstest where intf = "interface1" and node = "rtr.seat" ');

is(@$res, 1, "got 1 result");
$agg_values = $res->[0]{'agg_values'};
is(@$agg_values, 2, "2 results in aggregate query");
is($agg_values->[0][0], 0, "first timestamp is 0");
is($agg_values->[1][0], 604800, "second timestamp is 604800");

is(_get_day($agg_values->[1][0]), "Thursday", "week is aligned to Thursday");

#
# Okay now let's try to do "week alignment" instad of epoch second alignment.
# Instead of second alignment, this would be Monday to Monday aggregation of weeks
# Since this is a Thurs/Thurs timerange query we should end up once alinged Mon/Mon
#  with a partial fist bucket (Mon Dec 29-Jan5), full second bucket (Mon Jan 5-12), and partial bucket (Mon Jan 12-15)
$res = $query->run_query(query => 'get aggregate(values.output, 604800, count) align week as agg_values between("01/01/1970", "01/15/1970") from tsdstest where intf = "interface1" and node = "rtr.seat" ');

is(@$res, 1, "got 1 result");
$agg_values = $res->[0]{'agg_values'};
is(@$agg_values, 3, "3 results in aggregate query aligned to week");
is($agg_values->[0][0], -259200, "first timestamp is -259200 (Mon Dec 29)");
is($agg_values->[1][0], 345600, "second timestamp is 345600 (Mon Jan 5)");
is($agg_values->[2][0], 950400, "third timestamp is 950400 (Mon Jan 12)");

is(_get_day($agg_values->[0][0]), "Monday", "week is aligned to Mon");
is(_get_day($agg_values->[1][0]), "Monday", "week is aligned to Mon");
is(_get_day($agg_values->[2][0]), "Monday", "week is aligned to Mon");

is($agg_values->[0][1], 576, "got partial first bucket");
is($agg_values->[1][1], 1008, "got ful second bucket");
is($agg_values->[2][1], 432, "got partial third bucket");


# 
# Similar logic to the previous test, but we're going to try other alignments. Here is 'month'
$res = $query->run_query(query => 'get aggregate(values.output, 604800, count) align month as agg_values between("01/01/1970", "03/15/1970") from tsdstest where intf = "interface1" and node = "rtr.seat" ');

is(@$res, 1, "got 1 result");
$agg_values = $res->[0]{'agg_values'};
is(@$agg_values, 3, "3 results in aggregate query aligned to month");
is($agg_values->[0][0], 0, "first timestamp is 0 (Jan 1)");
is($agg_values->[1][0], 2678400, "second timestamp is 2678400 (Feb 1)");
is($agg_values->[2][0], 5097600, "third timestamp is 5097600 (Mar 1)");

is(_get_month($agg_values->[0][0]), "January", "month is aligned to Jan");
is(_get_month($agg_values->[1][0]), "February", "month is aligned to Feb");
is(_get_month($agg_values->[2][0]), "March", "month is aligned to Mar");

is($agg_values->[0][1], 4464, "31 days in January data");
is($agg_values->[1][1], 4032, "28 days in Feb 1970");
is($agg_values->[2][1], 144, "partial data in March");


# 
# Similar logic to the previous test, but we're going to try other alignments. Here is 'year'
# This one is a bit tricky since we only have about 3m worth of test data on some interfaces in the unit tests
$res = $query->run_query(query => 'get aggregate(values.output, 604800, count) align year as agg_values between("02/01/1970", "03/15/1970") from tsdstest where intf = "interface1" and node = "rtr.seat" ');


is(@$res, 1, "got 1 result");
$agg_values = $res->[0]{'agg_values'};
is(@$agg_values, 1, "1 results in aggregate query aligned to year");
is($agg_values->[0][0], 0, "first timestamp is 0 (Jan 1)");

is(_get_month($agg_values->[0][0]), "January", "month is aligned to Jan");
is(_get_year($agg_values->[0][0]), "1970", "year aligned to 1970");

is($agg_values->[0][1], 4608, "28 days in Feb + a 4 in March data");


sub _get_day {
    my $epoch = shift;
    return DateTime->from_epoch(epoch => $epoch)->day_name();
}

sub _get_month {
    my $epoch = shift;
    return DateTime->from_epoch(epoch => $epoch)->month_name();
}

sub _get_year {
    my $epoch = shift;
    return DateTime->from_epoch(epoch => $epoch)->year();
}

#BAIL_OUT('meow');


