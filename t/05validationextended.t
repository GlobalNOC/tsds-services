use strict;
use warnings;

use Test::More tests => 213;

# testing multiple where operators
use GRNOC::Config;
use GRNOC::TSDS::DataService::Query;
use Data::Dumper;
use FindBin;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $bnf_file = "$FindBin::Bin/../conf/query_language.bnf";
my $logging_file = "$FindBin::Bin/conf/logging.conf";

GRNOC::Log->new( config => $logging_file );

my $first_value;
my $last_value;
my $length;

sub validate_results{
    my ($result,$len) = @_;
    # validate total number of records sent back
    $length = scalar @$result;
    is($length,$len,"Count variable match with total number of output values returned by query");

    # validate the random values
    # # result =Multi Dimensional Array :::  index -> [interval at index 0] and [value at index 1 ] . Use index 1 for fetching value
    my $value= $result->[0]->[1]; # column 1 is for getting value
    ok($result->[0]->[1] < $result->[1]->[1] &&
       $result->[0]->[0] < $result->[1]->[0], "First row is less than later row");

    # Random row selection and validation .Random Seed generator
    # https://www.ccsf.edu/Pub/Perl/perlfunc/srand.html
    srand(time ^ $$ ^ unpack "%L*", `ps axww | gzip`);
    my $randnum = rand($length - 1);
    $value  = $result->[$randnum]->[1];
    my $value2 = $result->[$randnum+1]->[1];
    is($value2, $value + 1, " random row of query array result is valid ");
    $value=$result->[$length-1]->[1];
    ok($value >= $value2,"last row fetched of query array result is valid");
}

my $query = GRNOC::TSDS::DataService::Query->new( config_file => $config_file,
                                                  bnf_file => $bnf_file );
ok($query, "query data service connected");

# Testing data retrieval queries
# Fetching Input
my $arr= $query->run_query( query =>'get values.input between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ');
ok($arr, "query request to fetch values.input sent successfully");

my $result= $arr->[0]->{'values.input'};
ok( defined($result) , " query to fetch values of input fields (values.input) from Mongo successful ");

#validate the ouput array random index value
validate_results($result,4866);

# Fetching Output
$arr= $query->run_query( query =>'get values.output between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ');

ok($arr, "query request to fetch values.output sent successfully");
$result= $arr->[0]->{'values.output'};

ok( defined($result) , " query to fetch values of output fields (values.output) from Mongo successful ");
validate_results($result,4866);

# fetching input with renaming applied
# Each interface will have data from 1 to 4867.
$arr= $query->run_query( query =>'get values.input as IN between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where node="rtr.chic" and intf="interface4" ');
ok($arr, "query request to fetch values.input sent successfully");

$result= $arr->[0]->{'IN'};
ok( defined($result) , "query to fetch values of input fields (values.input as IN ) from Mongo successful ");
validate_results($result,4866);

# fetching output with renaming applied
$arr= $query->run_query( query =>'get values.output as OUT between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ');
ok($arr, "query request to fetch values.output sent successfully");

$result= $arr->[0]->{'OUT'};
ok( defined($result) , " query to fetch values of output fields (values.output) from Mongo successful ");
validate_results($result,4866);

# Check all aggregate points to ensure accuracy 
$arr= $query->run_query( query =>'get node, intf, aggregate(values.output, 300, average) as output, values.output between ("01/01/1970 00:00:00 UTC","01/01/1970 00:15:00 UTC") by node from tsdstest where node="rtr.chic" and intf="interface11" ordered by intf asc ');
ok($arr, "query request to fetch values.output by node sent successfully");
is(@$arr, 1, "got 1 result");
is($arr->[0]{'output'}[0][0], 0, "got aggregate timestamp");
is($arr->[0]{'output'}[0][1], 95055.5, "got aggregate value");
is($arr->[0]{'output'}[1][0], 300, "got aggregate timestamp");
is($arr->[0]{'output'}[1][1], 95085.5, "got aggregate value");
is($arr->[0]{'output'}[2][0], 600, "got aggregate timestamp");
is($arr->[0]{'output'}[2][1], 95115.5, "got aggregate value");
is($arr->[0]{'intf'}, "interface11");

# With Grouping By
$arr= $query->run_query( query =>'get node, intf, aggregate(values.output, 300, average) as output between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node, intf from tsdstest where node="rtr.chic" ordered by intf asc ');
ok($arr, "query request to fetch values.output by node sent successfully");
is(@$arr, 10, "got 10 results");
is($arr->[0]{'output'}[0][1], 103695.5, "got aggregate value");
is($arr->[0]{'intf'}, "ge-0/0/0");

# With Grouping by first
$arr= $query->run_query( query =>'get intf, node, aggregate(values.output, 300, average) as output between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node first(intf) from tsdstest where node="rtr.chic" ');
ok($arr, "query request to fetch values.output by meta.node sent successfully");
is(@$arr, 1, "got 1 result");
is($arr->[0]{'output'}[0][1], 103695.5, "got aggregate value");
is($arr->[0]{'intf'}, "ge-0/0/0", "got interface");

# Validate the results returned
# with ordering (Sort output)
$arr= $query->run_query( query =>'get values.output between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ordered by meta.intf,meta.node');
ok($arr, "query request to fetch values.output using order by meta.intf,meta.node sent successfully");

$result= $arr->[0]->{'values.output'};
ok( defined($result) , " query to fetch values of output fields (values.output) using ordered by meta.intf,meta.node from Mongo successful ");

$first_value= $result->[0]->[1];
is($first_value,103681,"First value of sorted output is valid ");

$length= scalar @$result - 1 ;
$last_value= $result->[$length]->[1];
is($last_value,108546,"Last value of sorted output is valid ");

# with ordering ( sort ascending default order)
$arr= $query->run_query( query =>'get values.input between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node="rtr.chic" ordered by meta.node');
ok($arr, "query request to fetch values.input using order by meta.node sent successfully");

$result= $arr->[0]->{'values.input'};
ok( defined($result) , " query to fetch values of input fields (values.input) using order by meta.node from Mongo successful ");

$first_value= $result->[0]->[1];
is($first_value,103681,"First value of sorted meta.node output is valid ");

$length= scalar @$result - 1 ;
$last_value= $result->[$length]->[1];
is($last_value,108546,"Last value of sorted meta.node output is valid ");

# subqueries
my $subquery ='get intf,node,values.input,values.output between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" ';
$arr=$query->run_query( query =>"get values.input from ($subquery)");
ok( defined($arr), "query request of simple sub query sent successfully");

$result = $arr->[0]->{'values.input'};
ok(defined($result), "query request of simple sub query executed successfully");
validate_results($result,4866);

# sub query and ordered by in main query
$arr = $query->run_query( query => "get values.input from ($subquery) ordered by node ");
ok(defined($arr), "subquery and ordered by condition in main query executed successfully");
validate_results($result,4866);

# sub query, with , ordered by example
$arr = $query->run_query( query => "get values.input,node from ($subquery) with details ordered by node ");
ok(defined($arr), "subquery and with, ordered by condition in main query executed successfully");
validate_results($result,4866);

# sub query with where condition
$arr = $query->run_query( query => "get values.input,node from ($subquery) where start > 0");
ok(defined($arr), "subquery and numerical where  condition in main query executed successfully");
validate_results($result,4866);

# sub query with where condition and with
$arr = $query->run_query( query => "get values.input,node from ($subquery) with details where start >0");
ok(defined($arr), "subquery , where condition  and with details in main query executed successfully");
validate_results($result,4866);

# sub query with where , with and ordered by operators
$arr = $query->run_query( query => "get values.input,node from ($subquery) with details where start > 0 ordered by node ");
ok(defined($arr), "subquery and with, ordered by condition and with details in main query executed successfully");
validate_results($result,4866);

$arr = $query->run_query( query => " get node from ($subquery) ");
$result = $arr->[0]->{'node'};
is($result,'rtr.chic'," Node value returned by subquery is valid");

$arr = $query->run_query( query => " get node from ($subquery) where node=\"rtr.chic\"  ");
$result = $arr->[0]->{'node'};
is($result,'rtr.chic'," Node value returned by subquery is valid");

# sub queries with additional field checks
$arr= $query->run_query( query => "get values.input,node by node from ($subquery) with details ordered by node ");
ok(defined($arr)," subquery with group by and order by executed successfully");

$result=$arr->[0]->{'node'};

is($result,'rtr.chic'," Node value returned by subquery is valid");

$arr= $query->run_query( query => " get intf,node by meta.node from ($subquery) with details ");
ok(defined($arr)," subquery with details and group by node to fetch intf and node executed successfully");

$arr= $query->run_query( query => " get intf,node by meta.node from ($subquery) with details ordered by node,intf");
ok(defined($arr)," subquery with details and group by node and intf to fetch intf and node executed successfully");

$arr= $query->run_query( query => " get values.input by meta.node from ($subquery) where start >0 ordered by node");
ok(defined($arr)," Subquery , conditional operator and sorted order by node query executed successfully");

$arr= $query->run_query( query => " get count(node) by meta.node from ($subquery) where start >0 ordered by node");
ok(defined($arr)," Subquery , count of node per each node group query executed successfully");

$arr= $query->run_query( query => " get values.output,node as NODENAME by meta.node from ($subquery) where start >0 ordered by node");
ok(defined($arr)," Subquery , output values and date check condition in main query grouped on each node  query executed successfully");

$arr= $query->run_query( query => " get values.output,node as NODENAME  by meta.node from ($subquery) where start >0 ordered by node");
ok(defined($arr)," Subquery , output values and column rename operation in main query grouped on each node  query executed successfully");

$arr= $query->run_query( query => " get aggregate(values.input, 3601, average) as Avg from ($subquery) ordered by node");
ok(defined($arr)," Subquery , aggregate(output values) and column rename operation in main query grouped on each node  query executed successfully");

$result=$arr->[0]->{'Avg'};

$arr= $query->run_query( query => " get average(values.input) from($subquery) ordered by node");
ok(defined($arr)," Subquery , average(output),sorted output and conditional statements query executed successfully");

$result=$arr->[0]->{'average(values.input)'};
is($result,"106113.5","Average value returned by query get average(values.input) from($subquery) ordered by node");

$arr= $query->run_query( query => "get max(values.input) from($subquery) ordered by node ");
ok(defined($arr),"Query with aggregate function , conditional statement , order by command and subquery executed successfully");

$result=$arr->[0]->{'max(values.input)'};
is($arr->[0]->{'max(values.input)'},"108546","Max value returned by query (get max(values.input) from($subquery) ordered by node) is valid ");

$arr= $query->run_query( query => "get sum(values.input) as SUM_OUTPUT from($subquery) ordered by node ");
ok(defined($arr),"Query with aggregate function , conditional statement , order by command and subquery executed successfully");

$result=$arr->[0]->{'SUM_OUTPUT'};
ok( defined($result) , "Aggregate function sum(field) result on sub query information returned succesfully");
is( $result ,516348291,"sum function return value verified ");

$arr= $query->run_query( query => " get min(values.input) as minval from($subquery) ");
ok(defined($arr),"Query with aggregate function min on input values and subquery executed successfully");
is($arr->[0]->{'minval'},"103681","Min value returned by query (get min(values.input) as minval from($subquery) ) is valid ");

$result=$arr->[0]->{'minval'};
ok( defined($result) , "Aggregate function min(field) result on sub query information returned succesfully");

$arr= $query->run_query( query => " get percentile(values.input, 95) as 95percentile from ($subquery) ordered by node ");
$result=$arr->[0]->{'95percentile'};
ok( defined($result) , " Percentile function result returned succesfully  on sub query information ");

$arr= $query->run_query( query => " get aggregate(values.input, 3601,min) as minval from ($subquery) ");
$result=$arr->[0]->{'minval'};
ok( defined($result) , " Aggregate function ( aggregate(values.input, 3601,min) ) result returned succesfully  on sub query information ");

$arr= $query->run_query( query => " get min(values.output) as minval from($subquery) ");
ok(defined($arr),"Query with aggregate function min on input values and subquery executed successfully");

$result=$arr->[0]->{'minval'};

ok( defined($result) , "Aggregate function min(field) output result on sub query information returned succesfully");
is($result,"103681","Min value returned by query (get min(values.output) as minval from($subquery) ) is valid ");

$arr= $query->run_query( query => " get max(values.output) as maxval from($subquery) ");
$result= $arr->[0]->{'maxval'};

ok( defined($result) , "Aggregate function max(field) output result on sub query information returned succesfully");
is($result,"108546","Max value returned by query (get max(values.output) as maxval from($subquery)) is valid ");

# Testing various interface values present in  particular node and values associated with it
$subquery ='get values.input,node,intf between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by intf from tsdstest where node = "rtr.chic" ';
$arr = $query->run_query( query => " get values.input,node,intf by intf from ($subquery) ordered by intf desc,node");
is($arr->[0]->{'intf'},"interface9","Interface at row 1 is validated");
is($arr->[1]->{'intf'},"interface8","Interface at row 2 is validated");
is($arr->[2]->{'intf'},"interface7","Interface at row 3 is validated");
is($arr->[3]->{'intf'},"interface6","Interface at row 4 is validated");
is($arr->[4]->{'intf'},"interface5","Interface at row 5 is validated");
is($arr->[5]->{'intf'},"interface4","Interface at row 6 is validated");
is($arr->[6]->{'intf'},"interface3","Interface at row 7 is validated");
is($arr->[7]->{'intf'},"interface11","Interface at row 8 is validated");
is($arr->[8]->{'intf'},"interface10","Interface at row 9 is validated");
is($arr->[9]->{'intf'},"ge-0/0/0","Interface at row 10 is validated");
validate_results($arr->[3]->{'values.input'},4866);

# Testing Subqueries with Groupby
# Testing various interface values present in  particular node and values associated with it
$subquery ='get values.input,node,intf between("01/01/1970 00:00:00 UTC","01/21/1970 13:31:00 UTC") by intf from tsdstest where node = "rtr.newy" ';
$arr = $query->run_query( query => " get values.input,node,intf by intf from ($subquery) ordered by intf desc,node");
is($arr->[0]->{'intf'},"xe-0/1/0.0","Interface at row 1 is validated");
is($arr->[1]->{'intf'},"interface9","Interface at row 2 is validated");
is($arr->[2]->{'intf'},"interface8","Interface at row 3 is validated");
is($arr->[3]->{'intf'},"interface7","Interface at row 4 is validated");
is($arr->[4]->{'intf'},"interface6","Interface at row 5 is validated");
is($arr->[5]->{'intf'},"interface5","Interface at row 6 is validated");
is($arr->[6]->{'intf'},"interface4","Interface at row 7 is validated");
is($arr->[7]->{'intf'},"interface3","Interface at row 8 is validated");
is($arr->[8]->{'intf'},"interface11","Interface at row 9 is validated");
is($arr->[9]->{'intf'},"interface10","Interface at row 10 is validated");
#validate_results($arr->[9]->{'values.input'},4866);

# Testing interface values array and nodes associated with it
$subquery ='get values.output,node,intf between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node,intf from tsdstest where intf = "interface3" ';
$arr= $query->run_query( query => " get values.output,node,intf from ($subquery) where node=\"rtr.chic\"  ordered by intf");
is($arr->[0]->{'node'},"rtr.chic","Node value returned by query ( get values.input,node,intf from ($subquery) where node=\"rtr.chic\"  ordered by intf) is validated");
is($arr->[0]->{'intf'},"interface3","Interface value returned by query ( get values.input,node,intf from ($subquery) where node=\"rtr.chic\"  ordered by intf) is validated");
validate_results($arr->[0]->{'values.output'},4866);


$subquery ='get intf,node,values.input,values.output between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by intf from tsdstest where node = "rtr.chic" ';
$arr= $query->run_query( query => " get intf,node,min(values.output) as Min_Val,average(values.input),average(values.output) as AVGOUT by intf from ($subquery) ");
is($arr->[0]->{'AVGOUT'},88833.5,"Average returned by query is valid");
is($arr->[0]->{'Min_Val'},86401,"Minimum value returned by query is valid");

$subquery ='get values.output,node,intf between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node,intf from tsdstest where intf = "interface3" ordered by node';
$arr= $query->run_query( query => "get intf,node by node from ($subquery) ");
$length= scalar @$arr -1;

is($arr->[0]->{'intf'},"interface3"," Interface value retrieved correctly");
is($arr->[0]->{'node'},"rtr.chic","Node value rtr.chic retrieved correctly");

is($arr->[1]->{'intf'},"interface3"," Interface value interface5 retrieved correctly");
is($arr->[1]->{'node'},"rtr.newy","Node value rtr.newy retrieved correctly");


# multiple subqueries test
$subquery ='get values.output,node,intf between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node,intf from tsdstest where intf = "interface3" ';
my $subquery2 ="get node,intf by node,intf from ($subquery) where intf = \"interface3\" ";

$arr= $query->run_query( query => " get node,intf from ($subquery2) where node=\"rtr.chic\" ");
is($arr->[0]->{'node'},"rtr.chic","Node value rtr.chic retrieved by queries with multiple sub queries correctly");

my $subquery3= "get node,intf from ($subquery2) where node=\"rtr.chic\" ";

$arr= $query->run_query( query => " get node,intf from ($subquery3) ");
is($arr->[0]->{'intf'},"interface3"," Interface value retrieved succesfully by query with multiple sub queries ");
is($arr->[0]->{'node'},"rtr.chic", "Node value rtr.chic retrieved by queries with multiple sub queries correctly");

$arr= $query->run_query( query => " get node,intf by node from ($subquery2) ordered by node desc");
is($arr->[0]->{'node'},"rtr.newy", "Node value rtr.newy retrieved by queries with multiple sub queries correctly");
is($arr->[1]->{'node'},"rtr.chic", "Node value rtr.chic retrieved by queries with multiple sub queries correctly");

# Testing limit and offset with sub queries
$arr=$query->run_query( query => "get node,intf by node from ( $subquery)  limit 1 offset 0 ordered by node");
is($arr->[0]->{'node'},"rtr.chic", " Limit and offset to retrieve first record is validated ");

$subquery ='get values.output,node,intf between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node,intf from tsdstest where node="rtr.chic" ';
$arr=$query->run_query( query => "get node,intf by intf from ( $subquery)  limit 10 offset 0 ordered by intf");
is($arr->[0]->{'node'},"rtr.chic","Node value retrieved is valid");
$length = scalar @$arr -1;
is($arr->[$length]->{'intf'},"interface9","Interface value and order retrieved is valid");
is($arr->[0]->{'intf'},"ge-0/0/0","Interface first value and order retrieved is valid");

$arr=$query->run_query( query => "get node,intf by intf from ( $subquery)  limit 10 offset 9 ordered by intf desc");
is($arr->[0]->{'intf'},"ge-0/0/0","Interface last value and order descending retrieved is valid");

$arr=$query->run_query( query => "get node,intf by intf from ( $subquery)  limit 1 offset 0 ordered by intf desc");
is($arr->[0]->{'intf'},"interface9","Interface first value and order descending retrieved is valid");


$subquery ='get node,intf between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by intf, node from tsdstest where intf like "interface" limit 5  offset 5 ordered by intf desc';
$arr=$query->run_query( query => "get node,intf by intf from ( $subquery) limit 3 offset 0  ordered by intf");
is($arr->[0]->{'intf'},"interface5","Interface first value and order descending retrieved is valid");
is($arr->[1]->{'intf'},"interface6","Interface first value and order descending retrieved is valid");
is($arr->[2]->{'intf'},"interface7","Interface first value and order descending retrieved is valid");

# Testing aggreagate functions with various bucket input
$arr=$query->run_query(query => 'get values.input,node,aggregate(values.input,300,average) as AVGIN,values.output,aggregate(values.output,600,min) as MINOUT  between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf="interface5" ');
is($arr->[0]->{'MINOUT'}->[0]->[1],17281,"Minimum value in first bucket validated successfully");

$arr=$query->run_query(query => 'get aggregate(values.input,600,count) as BucketCount between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf="interface5" ');


# Testing Math operations
$arr= $query->run_query( query =>'get aggregate(values.input, 1, average) - 20 as inputminus20,intf between("01/01/1970 00:00:00 UTC","01/10/1970 13:31:00 UTC") by intf from tsdstest limit 2 offset 1');
my $len= scalar @$arr -1;
$result= $arr->[0]->{'inputminus20'};

my $count=0;
my $i=0;
while($i <= $len){
    if( defined $arr->[$i]->{'intf'}) {
        $count++;
    }
    $i++;
}

is($count,2,"Two interface values returned as per limit value specified in the query");
$len= scalar @$result -1;

my $arr2= $query->run_query( query =>'get aggregate(values.input, 1, average) as input,intf between("01/01/1970 00:00:00 UTC","01/10/1970 13:31:00 UTC") by intf from tsdstest limit 2 offset 1');
my $result2=$arr2->[0]->{'input'};
my $len2= scalar @$result2 -1;
is($len,$len2, " Length of math operation query and length of query with same logic expect math operation is different. Length of response should be same as we are just computing some math operation on response set");

# now validate data and the difference between the fields
my $mid = int($len2 /2);

#warn "MID = $mid";
#warn "sizeof array1 = " .scalar(@$result);
#warn "sizeof array2 = " . scalar(@$result2);
#warn Dumper($result);
#die Dumper($result2);

is( int($result2->[0]->[1] - $result->[0]->[1]), 20 , " Difference between query with actual data and query that decrements value by 20 should be 20.Validated the difference between them at first row");
is( int($result2->[$mid]->[1] - $result->[$mid]->[1]), 20 , " Difference between query with actual data and query that decrements value by 20 should be 20.Validated the difference between them at middle row");

$arr= $query->run_query( query =>'get aggregate(values.input, 1, average) * 20 as inputmultiply20,intf between("01/01/1970 00:00:00 UTC","01/10/1970 13:31:00 UTC") by intf from tsdstest limit 2 offset 1');
$result = $arr->[0]->{'inputmultiply20'};

is( int($result->[0]->[1] / $result2->[0]->[1]), 20 , " Division value between query with actual data and query that multiplies value by 20 should be 20.Validated the difference between them at first row");
is( int($result->[$mid]->[1] / $result2->[$mid]->[1]), 20 , " Division value between query with actual data and query that multiplies value by 20 should be 20.Validated the difference between them at middle row");

$arr= $query->run_query( query =>'get aggregate(values.input, 1, average) / 20 as inputdiv20,intf between("01/01/1970 00:00:00 UTC","01/10/1970 13:31:00 UTC") by intf from tsdstest limit 2 offset 1');
$result = $arr->[0]->{'inputdiv20'};

is( int($result2->[0]->[1] / $result->[0]->[1]), 20 , " Division value between query with actual data and query that divides value by 20 should be 20.Validated the difference between them at first row");
is( int($result2->[$mid]->[1] / $result->[$mid]->[1]), 20 , " Division value between query with actual data and query that divides value by 20 should be 20.Validated the difference between them at middle row");

$arr= $query->run_query( query =>'get aggregate(values.input, 1, average) + 20 as inputplus20,intf between("01/01/1970 00:00:00 UTC","01/10/1970 13:31:00 UTC") by intf from tsdstest limit 2 offset 1');
$result = $arr->[0]->{'inputplus20'};

is( int($result2->[0]->[1] - $result->[0]->[1]), -20 , " Value difference between query with actual data and query that increments value by 20 should be 20.Validated the difference between them at first row");
is( int($result2->[$mid]->[1] - $result->[$mid]->[1]), -20 , " Value difference between query with actual data and query that increments value by 20 should be 20.Validated the difference between them at middle row");


# Test "having" clause
my $base = $query->run_query( query =>'get average(values.input) as avg, intf between("01/01/1970 00:00:00 UTC","01/10/1970 13:31:00 UTC") by intf from tsdstest where node="rtr.chic"');

ok($base, "query request with having sent successfully");
is(@$base, 10, "got all 10 interfaces response");

$arr = $query->run_query( query =>'get average(values.input) as avg, intf between("01/01/1970 00:00:00 UTC","01/10/1970 13:31:00 UTC") by intf from tsdstest where node="rtr.chic" having intf like "ge"');
ok($arr, "query request with having sent successfully");
is(@$arr, 1, "got all 1 interfaces response with having");
is($arr->[0]->{'intf'}, "ge-0/0/0", "got correct interface");
is($arr->[0]->{'avg'}, "108000.5", "got correct interface data");

$arr = $query->run_query( query =>'get average(values.input) as avg, intf between("01/01/1970 00:00:00 UTC","01/10/1970 13:31:00 UTC") by intf from tsdstest where node="rtr.chic" having intf like "ge" or (intf like "interface1" and avg >= 4000) ordered by intf');
ok($arr, "query request with having sent successfully");
is(@$arr, 3, "got all 3 interfaces response with having");
is($arr->[0]->{'intf'}, "ge-0/0/0", "got correct interface");
is($arr->[1]->{'intf'}, "interface10", "got correct interface");
is($arr->[2]->{'intf'}, "interface11", "got correct interface");


# Test $field $op $field in get clause
$arr = $query->run_query( query =>'get sum(values.input) - sum(values.output) as diff_minus, sum(values.input) / sum(values.output) as diff_divide, sum(values.input) + sum(values.output) as diff_plus,  sum(values.input) * sum(values.output) as diff_mult, intf between("01/01/1970 00:00:00 UTC","01/10/1970 13:31:00 UTC") by intf from tsdstest where node="rtr.chic"');

ok($arr, "query request with having sent successfully");
is(@$arr, 10, "got all 10 interfaces response");
is($arr->[0]->{'diff_minus'}, 0, "got right diff minus");
is($arr->[0]->{'diff_divide'}, 1, "got right diff divide");
is($arr->[0]->{'diff_plus'}, 1567650240, "got right diff plus");
is($arr->[0]->{'diff_mult'}, 614381818743014400, "got right diff mult");


# Test $field $op numeric value
# First is a number / 10, second is a series * 2
my $arr = $query->run_query( query =>'get sum(values.input) as summed, sum(values.input) / 10 as divided, values.input as base, values.input * 2 as multiplied, intf between("01/01/1970 00:00:00 UTC","01/10/1970 00:00:00 UTC") by intf from tsdstest where node="rtr.chic"');

ok($arr, "query request with having sent successfully");
is(@$arr, 10, "got all 10 interfaces response");
is($arr->[0]->{'summed'}, 783825120, "got summed value correctly");
is($arr->[0]->{'divided'}, 78382512, "got sum divided value correctly");

# base and multiplied should have the same number of elements, and each item in multiplied
# should be its corresponding position item in base times 2
my $base = $arr->[0]->{'base'};
my $multiplied = $arr->[0]->{'multiplied'};
ok(@$base == 9000 && @$base == @$multiplied, "base and multiplied arrays are same size");

my $good = 1;
for (my $i = 0; $i < @$base; $i++){
    my $base_val = $base->[$i][1];
    my $scaled_val = $multiplied->[$i][1];

    next if (! defined $base_val);
    $good = $good && (($base_val * 2) == $scaled_val);
}
is($good, 1, "base and multiplied points are correctly scaled");



# Test $field $op $metadata
# First is a number / 10, second is a series / max_bandwidth

#######
# TODO: Would be great not to have this hack reach in and set metadata, consider
# adding a test library maybe to make it easy to temporarily change things like this
#######
my $measurements = $query->parser()->mongo_rw()->get_database('tsdstest')->get_collection('measurements');
my $hack_doc = $measurements->find_one({'node' => 'rtr.chic', 'intf' => 'ge-0/0/0'});
$measurements->update_one({_id => $hack_doc->{'_id'}}, {'$set' => {'max_bandwidth' =>'1000000000'}});
my $arr = $query->run_query( query =>'get max_bandwidth, values.input as base, values.input / max_bandwidth  as scaled, intf between("01/01/1970 00:00:00 UTC","01/10/1970 00:00:00 UTC") by intf from tsdstest where node="rtr.chic" and intf = "ge-0/0/0"');
$measurements->update_one({_id => $hack_doc->{'_id'}}, {'$unset' => {'max_bandwidth' => 1}});


ok($arr, "query request with having sent successfully");
is(@$arr, 1, "got 1 interface response");


# base and multiplied should have the same number of elements, and each item in multiplied
# should be its corresponding position item in base times 2
my $base = $arr->[0]->{'base'};
my $scaled = $arr->[0]->{'scaled'};
my $bandwidth = $arr->[0]->{'max_bandwidth'};
ok(@$base == 9000 && @$base == @$multiplied, "base and scaled arrays are same size");

my $good = 1;
for (my $i = 0; $i < @$base; $i++){
    my $base_val = $base->[$i][1];
    my $scaled_val = $scaled->[$i][1];

    next if (! defined $base_val);
    $good = $good && (($base_val / $bandwidth) == $scaled_val);
}
is($good, 1, "base and scaled points are correctly scaled");


# Test multiple embedded operators on single field
$arr = $query->run_query( query =>'get count(min(values.input)) as first, average(aggregate(values.input, 3600, average)) as second, max(average(aggregate(values.input, 3600, average))) as third, count(max(average(aggregate(values.input, 3600, average)))) as fourth, sum(count(max(average(aggregate(values.input, 3600, average))))) as fifth between ("01/01/1970 00:00:00 UTC","01/01/1970 12:00:00 UTC") from tsdstest where intf = "ge-0/0/0" and node = "rtr.chic" ');
ok($arr, "query request to fetch values.input sent successfully");

is($arr->[0]->{'first'}, 1, "got count min chain");
is($arr->[0]->{'second'}, 105840.5, "got average aggregate chain");
is($arr->[0]->{'third'}, 105840.5, "got max average aggregate chain");
is($arr->[0]->{'fourth'}, 1, "got count max average aggregate chain");
is($arr->[0]->{'fourth'}, 1, "got sum count max average aggregate chain");


# Test selecting by a complex field name, this changed in Mongo 3.4 and we have
# to ensure we're properly encoding/decoding the field in the query engine
$arr = $query->run_query( query =>'get values.input, circuit.name between ("01/01/1970 00:00:00 UTC","01/01/1970 12:00:00 UTC") by circuit.name, circuit.description from tsdstest where circuit.name = "circuit2" limit 5 offset 0 ');
ok($arr, "complex field name handled correctly");



# Test that aggregate(... sum) works
$arr = $query->run_query( query =>'get aggregate(values.input, 1, sum) as sum, all(intf) as intfs between ("01/01/1970 00:00:00 UTC","01/01/1970 01:00:00 UTC") from tsdstest where node = "rtr.chic" ');
ok($arr, "query request to fetch values.input sent successfully");

is(@{$arr->[0]->{'sum'}}, 360, "got right number of data points");
is(@{$arr->[0]->{'intfs'}}, 10, "got right number of intfs");
is($arr->[0]->{'sum'}[0][0], 0, "got first ts");
is($arr->[0]->{'sum'}[0][1], 1252810, "got first val");
is($arr->[0]->{'sum'}[1][0], 10, "got second ts");
is($arr->[0]->{'sum'}[1][1], 1252820, "got second val");
is($arr->[0]->{'sum'}[-1][0], 3590, "got last ts");
is($arr->[0]->{'sum'}[-1][1], 1256400, "got last val");

