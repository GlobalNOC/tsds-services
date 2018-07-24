use strict;
use warnings;
use Test::More tests => 81;
use GRNOC::Config;
use GRNOC::Log;
use GRNOC::TSDS::DataService::Query;
use Data::Dumper;
use List::MoreUtils qw(all);
use FindBin;

my $config_file  = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";
my $bnf_file = "$FindBin::Bin/../conf/query_language.bnf";

GRNOC::Log->new( config => $logging_file );
my $query = GRNOC::TSDS::DataService::Query->new( config_file => $config_file,
                                                  bnf_file => $bnf_file );
ok($query, "query data service connected");

# Testing aggregate functions like count,min,max and average

# Testing Average function
my $arr= $query->run_query( query =>'get average(values.output) between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" ');
ok($arr, "query request sent successfully");

my $result= $arr->[0]->{'average(values.output)'};
ok( defined($result) , "query to fetch average output field from Mongo successful");

#validate the result returned
is( $result ,106113.5, " average function return value verified ");

# testing other functions like count
$arr= $query->run_query( query =>'get count(values.input) as Count_Input between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node = "rtr.chic" ');
ok($arr," query request to get count of input sent successfully");

$result= $arr->[0]->{'Count_Input'};
ok( defined($result) , "query to fetch count of input fields from Mongo successful");

# comparing the value returned with actual value
is( $result ,4866, " count function return value verified ");

# testing max function
$arr= $query->run_query( query =>'get max(values.output) between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" ');
ok($arr, "query request to get max (output) sent successfully");

$result= $arr->[0]->{'max(values.output)'};
ok( defined($result) , "query to fetch max(values.output) of input fields from Mongo successful");

# comparing the value returned with actual value
is( $result ,108546, "max function return value verified ");

# testing min function
$arr= $query->run_query( query =>'get min(values.output) between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ');
ok($arr, "query request to get min (output) sent successfully");

$result= $arr->[0]->{'min(values.output)'};
ok( defined($result) , "query to fetch min(values.output) of input fields from Mongo successful");

# comparing the value returned with actual value
is( $result ,103681 , "min function return value verified ");

# testing sum function
$arr= $query->run_query( query =>'get sum(values.input) as SUM between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ');
ok($arr, "query request to get sum (values.input) sent successfully");

$result=$arr->[0]->{'SUM'};
ok(defined($result), "query request to get sum (values.input) executed successfully");

# comparing the value returned with actual value
is( $result,516348291,"sum function return value verified ");

# testing histogram function
$arr= $query->run_query( query =>'get histogram(values.input,100000) as Histogram between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ');
ok($arr, "query request to get histogram(values.input,1) as Histogram  sent successfully");

$result=$arr->[0]->{'Histogram'};
ok(defined($result), "query request to get histogram(values.input,1,0.2,3) as Histogram executed successfully");

# validate result values  of histogram

# testing percentile function
$arr= $query->run_query( query =>'get percentile(values.output, 95) between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ');
ok($arr, "query request to get percentile(values.output,95) sent successfully");

$result=$arr->[0]->{'percentile(values.output, 95)'};
ok(defined($result), "query request to get percentile(values.output,95) executed successfully");
is( $result ,'108303', " percentile function return value verified ");

$arr= $query->run_query( query =>'get percentile(values.output, 90) between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC")  from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ');
ok($arr, "query request to get percentile(values.output,90) sent successfully");

$result=$arr->[0]->{'percentile(values.output, 90)'};
ok(defined($result), "query request to get percentile(values.output,90) executed successfully");
is( $result ,'108060', " percentile function return value verified ");

$arr= $query->run_query( query =>'get percentile(values.output, 80) between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC")  from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ');
ok($arr, "query request to get percentile(values.output,80) sent successfully");

$result=$arr->[0]->{'percentile(values.output, 80)'};
ok(defined($result), "query request to get percentile(values.output,80) executed successfully");
is( $result ,'107573', " percentile function return value verified ");

# testing extrapolate
$arr= $query->run_query( query =>'get extrapolate(values.output, 1000) between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ');
ok($arr, "query request to get extrapolate(values.output, 1000) sent successfully");

$result=$arr->[0]->{'extrapolate(values.output, 1000)'};
ok(defined($result), "query request to get extrapolate(values.output,1000) executed successfully");
is($result,'-1026810', " Extrapolate result is valid ");

# testing explorate version 2
$arr= $query->run_query( query =>'get extrapolate(values.output, "01/02/2014") as extrapolate_output between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ');
ok($arr, "query request to get extrapolate(values.output, '01/02/2014') sent successfully");

$result=$arr->[0]->{'extrapolate_output'};
ok(defined($result), "query request to get extrapolate(values.output, '01/02/2014') as extrapolate_output executed successfully");
# need to validate values returned by it
is($result,"138965761","Extrapolate value for input 01/02/2014 is validated");

# few tests on extrapolate linear fit
$arr= $query->run_query( query =>'get extrapolate(values.output, "01/02/1970") as extrapolate_output between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ');
$result=$arr->[0]->{'extrapolate_output'};
is($result,"112321","Extrapolate value for input 01/02/1970 is validated");

$arr= $query->run_query( query =>'get extrapolate(values.output, "10/01/1997 00:15:00 UTC") as extrapolate_valuesoutput between ("01/01/1970 00:00:00 UTC","01/01/1970 00:11:00 UTC") from tsdstest where intf = "xe-0/1/0.0" and node="rtr.newy" ');
$result=$arr->[0]->{'extrapolate_valuesoutput'};
is($result,"87618331","Extrapolate value for input 10/01/1997 00:15:00 UTC is validated");

# test the option to have extrapolations as a series, rather than single points
$arr= $query->run_query( query =>'get extrapolate(values.output, series) as extrap_series between ("01/01/1970 00:01:00 UTC", "01/01/1970 00:04:10 UTC") from tsdstest where intf="ge-0/0/0" and node="rtr.chic"' );
ok(defined($arr) && defined($arr->[0]) && defined($arr->[0]->{'extrap_series'}), 'Extrapolation series 1 exists');
$result=$arr->[0]->{'extrap_series'};
is($result->[0][0], '60', 'Extrapolation series 1 starts at right time');
is($result->[-1][0], '250', 'Extrapolation series 1 ends at right time');
is(scalar(@$result), '20', 'Extrapolation series 1 has correct number of points');
ok((all { $result->[$_][0] < $result->[$_+1][0] } (0..(scalar(@$result)-2))), 'Extraplation series 1: data points occur in right order');
is($result->[0][1], '103687', 'Extrapolation series 1 has correct first point');
is($result->[-1][1], '103706', 'Extrapolation series 1 has correct last point');

# extrapolations as a series, on an actual extrapolation of data
$arr= $query->run_query( query =>'get extrapolate(values.output, series) as exts between("01/01/1970 23:59:00 UTC", "01/02/1970 00:03:00 UTC") from tsdstest where node="rtr.newy" and intf="interface6"' );
ok(defined($arr) && defined($arr->[0]) && defined($arr->[0]->{'exts'}), 'Extrapolation series 2 exists');
$result=$arr->[0]->{'exts'};
is($result->[0][0], '86340', 'Extrapolation series 2 starts at right time');
is($result->[-1][0], '86580', 'Extrapolation series 2 ends at right time');
is(scalar(@$result), '20', 'Extrapolation series 2 has correct number of points');
ok((all { $result->[$_][0] < $result->[$_+1][0] } (0..(scalar(@$result)-2))), 'Extraplation series 2: data points occur in right order');
is($result->[0][1], '77755', 'Extrapolation series 2 has correct first point');
is($result->[-1][1], '77779', 'Extrapolation series 2 has correct last point');

# test extrapolations as a series, over an interval of under 20 seconds
$arr= $query->run_query( query =>'get extrapolate(values.output, series) as extrap_series between ("01/01/1970 23:59:39 UTC", "01/01/1970 23:59:53 UTC") from tsdstest where intf="ge-0/0/0" and node="rtr.chic"' );
ok(defined($arr) && defined($arr->[0]) && defined($arr->[0]->{'extrap_series'}), 'Extrapolation series 3 exists');
$result=$arr->[0]->{'extrap_series'};
is($result->[0][0], '86379', 'Extrapolation series 3 starts at right time');
is($result->[-1][0], '86393', 'Extrapolation series 3 ends at right time');
is(scalar(@$result), '15', 'Extrapolation series 3 has correct number of points');
ok((all { $result->[$_][0] < $result->[$_+1][0] } (0..(scalar(@$result)-2))), 'Extraplation series 3: data points occur in right order');
ok(abs($result->[0][1] - 112318.9) < 1e-6, 'Extrapolation series 3 has correct first point');
ok(abs($result->[-1][1] - 112320.3) < 1e-6, 'Extrapolation series 3 has correct last point');

# percentile over max function
$arr= $query->run_query( query => 'get percentile(aggregate(values.input,720,max),90) as precentilehist between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where node="rtr.chic" ');
$result = $arr->[0]->{'precentilehist'};
ok(defined($result), " query to compute percentile over aggregate max function executed successfully");

# fetch all the values (sub query output) and compute 90 percentile over data.Compare the results

$arr= $query->run_query( query => 'get aggregate(values.input,720,max) as result between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where node="rtr.chic" ');
my $result2= $arr->[0]->{'result'};
my $len=scalar @$result2; # total number of slots in the array
my $required =  int( ($len * 90) /100);  # compute 90% slot
my $value= $result2->[$required]->[1]; # get its value

is($result,$value, " 90 percentile on aggregate values ( aggregate(values.input,720,max) ) is valid");

# compound statement with more than one aggregate function

$arr= $query->run_query( query =>'get max(aggregate(values.input,360,max)) as MAXMAX between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where node="rtr.chic" ');
ok($arr, "query request to get max(aggregate(values.input,360,max))  sent successfully");

$result=$arr->[0]->{'MAXMAX'};
ok(defined $result, "Compound statement max (aggregate function with max) executed and value returned back successfully ");
is( $result,169056, "Compound statement max(aggregate function with  max) executed and returned value is validated");

$arr= $query->run_query( query =>'get sum(aggregate(values.input,360,max)) as MAXSUM between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by intf from tsdstest where node="rtr.chic" ');
ok($arr, "query request to get sum (aggregate(values.input,360,max)) ) sent successfully");

$result=$arr->[0]->{'MAXSUM'};
ok(defined $result, "Compound statement sum(aggregate function with max) executed and value returned back successfully ");

$arr= $query->run_query( query =>'get aggregate(values.input,7200,max) as MAXVALUE between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where node="rtr.chic" ');
$result=$arr->[0]->{'MAXVALUE'};
validatebucket($result,'7200');

$arr= $query->run_query( query =>'get average(aggregate(values.input,7200,max)) as AVGWithMAX, aggregate(values.input,7200,max) between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where node="rtr.chic" ');
$result=$arr->[0]->{'AVGWithMAX'};

is(int($result),167040,"Compound statement average(aggregate function with  max) executed and returned value is validated");

# aggregate function to compute  histogram
$arr=$query->run_query( query => 'get aggregate(values.input,7200,histogram) as hists between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node,intf from tsdstest where node="rtr.chic" and intf="ge-0/0/0" ');
$result = $arr->[0]->{'hists'};
ok(defined($result), "aggregate function query to compute histogram executed successfully ");


# aggregate 1 day, should be exactly 24 data points
$arr=$query->run_query( query => 'get count(aggregate(values.input,3600,average)) as avg_count between ("01/01/1970 00:00:00 UTC","01/02/1970 00:00:00 UTC") by node,intf from tsdstest where node="rtr.chic" and intf="ge-0/0/0" ');
$result = $arr->[0]->{'avg_count'};
ok(defined($result), "aggregate function query to compute histogram executed successfully ");
is($result, 24, "got 24 hour buckets");

# percentile over histogram function
TODO: {

    local $TODO = "ISSUE=12710";

    $arr = $query->run_query( query => 'get percentile(hists,95) as 95percentilehist from (get aggregate(values.input,7200,histogram) as hists between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node,intf from tsdstest where node="rtr.chic" and intf="ge-0/0/0" )' );

$result = $arr->[0]{'95percentilehist'};
ok(defined($result), " query to compute percentile over histogram aggregate function executed successfully");

is(int($result),"500000000","95 percentile  computed using histogram values is valid ");

$arr = $query->run_query( query => 'get percentile(hists,90) as 90percentilehist from (get aggregate(values.input,7200,histogram) as hists between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node,intf from tsdstest where node="rtr.chic" and intf="ge-0/0/0" )' );
$result = $arr->[0]{'90percentilehist'};
ok(defined($result), " query to compute percentile over histogram aggregate function executed successfully");

is(int($result),"500000000","90 percentile  computed using histogram values is valid ");
}

# Validate the aggrgate functions bucket used and max value in each bucket will be compared
sub validatebucket{
    my $result=shift;
    my $interval=shift;
    my $len=scalar @$result-1;
    for (my $i=0;$i<$len-1; $i++){
        # get values of current bucket and next bucket and compute difference between them to validate intervals
        my $temp1= $result->[$i]->[0];
        my $temp2= $result->[$i+1]->[0];
        my $diff =  $temp2 - $temp1;
        is($diff,$interval,"Aggrgate bucket size is valid");
        $temp1= $result->[$i]->[1];
        $temp2= $result->[$i+1]->[1];
        $diff =  $temp2 - $temp1;
        is($diff,int($interval/10),"Aggrgate bucket value is valid");
    }
}


