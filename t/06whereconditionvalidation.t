use strict;
use warnings;
use Test::More tests => 92;
use GRNOC::Config;
use GRNOC::TSDS::DataService::Query;
use Data::Dumper;
use FindBin;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $bnf_file = "$FindBin::Bin/../conf/query_language.bnf";

my $query = GRNOC::TSDS::DataService::Query->new( config_file => $config_file,
                                                  bnf_file => $bnf_file );

ok($query, "query data service connected");

my $arr;
my $result;
my $len;

sub validate_results{
    my ($result,$len) = @_;
    # validate total number of records sent back
    my  $length = scalar @$result;
    is($length,$len,"Count variable match with total number of output values returned by query");

    # validate the random values
    # # result =Multi Dimensional Array :::  index -> [interval at index 0] and [value at index 1 ] . Use index 1 for fetching value
    my $value= $result->[0]->[1]; # column 1 is for getting value
    is($value, 1, " First row fetched by query is valid ");

    # Random row selection and validation .Random Seed generator
    # https://www.ccsf.edu/Pub/Perl/perlfunc/srand.html
    srand(time ^ $$ ^ unpack "%L*", `ps axww | gzip`);
    my $randnum = rand($length);
    $value= $result->[$randnum]->[1];
    is($value, int($randnum + 1) , " random row fetched by query is valid ");

    $value= $result->[$length-1]->[1];
    is($value,$len, " Last row fetched by query is valid");
}

# Using IN operator with where condition
$arr= $query->run_query( query =>'get intf,values.input,node between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0" and node in("rtr.chic","rtr.newy")');
ok($arr, "query request to fetch values.input sent successfully");

$result= $arr->[0]->{'intf'};
is($result,'ge-0/0/0'," Interface value returned by using intf = 'ge-0/0/0' and node in('rtr.chic','rtr.newy') query is valid .");

$result= $arr->[0]->{'values.input'};
ok( defined($result), " Query to fetch values of input using IN operator in where condition from Mongo successfully executed ");

$result= $arr->[0]->{'node'};
is($result,'rtr.chic',"Node value returned by using intf = 'ge-0/0/0' and node in('rtr.chic','rtr.newy') query is valid .");

validate_results($arr->[0]->{'values.input'},4867);


# using In and By operators
$arr= $query->run_query( query =>'get intf,values.input,node between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node  from tsdstest where node in("rtr.chic","rtr.newy")  and intf in ("xe-0/1/0.0","ge-0/0/0") ordered by node desc');

$len= scalar @$arr - 1;

is($arr->[0]->{'intf'},'xe-0/1/0.0',' First Interface value returned by query (get intf,values.input,node between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node  from tsdstest where node in("rtr.chic","rtr.newy") ordered by node) is valid');

is($arr->[0]->{'node'},'rtr.newy','First Node value returned by query (get intf,values.input,node between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node  from tsdstest where node in("rtr.chic","rtr.newy") ordered by node) is valid');

validate_results($arr->[0]->{'values.input'},4867);


is($arr->[$len]->{'intf'},'ge-0/0/0',' First Interface value returned by query (get intf,values.input,node between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node  from tsdstest where node in("rtr.chic","rtr.newy") ordered by node) is valid');

is($arr->[$len]->{'node'},'rtr.chic','First Node value returned by query (get intf,values.input,node between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node  from tsdstest where node in("rtr.chic","rtr.newy") ordered by node) is valid');

validate_results($arr->[$len]->{'values.input'},4867);


# different query that will return node rtr.newy and intf xe-0/1/0.0
$arr= $query->run_query( query =>'get intf,values.input,node between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "xe-0/1/0.0" and node in("rtr.chic","rtr.newy")');
ok($arr, "query request to fetch values.input sent successfully");

$result= $arr->[0]->{'intf'};
is($result,'xe-0/1/0.0'," Interface value returned by using intf = 'xe-0/1/0.0' and node in('rtr.chic','rtr.newy') query is valid .");

$result= $arr->[0]->{'values.input'};
ok( defined($result), " Query to fetch values of input using IN operator in where condition from Mongo successfully executed ");

$result= $arr->[0]->{'node'};
is($result,'rtr.newy',"Node value returned by using intf = 'xe-0/1/0.0' and node in('rtr.chic','rtr.newy') query is valid .");

validate_results($arr->[0]->{'values.input'},4867);

# Different Date Format Test for between clause
$arr= $query->run_query( query =>'get values.input between ("01/01/1970","01/10/1970") from tsdstest where intf = "ge-0/0/0" and node ="rtr.chic" ');
ok($arr, "query request to fetch values.input  with date format (MM/DD/YYYY) sent successfully");

$result= $arr->[0]->{'values.input'};
ok( defined($result), " query to fetch values of input with date format (MM/DD/YYYY) from Mongo successful ");

$arr= $query->run_query( query =>'get values.input between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC")  from tsdstest where intf = "ge-0/0/0" and node ="rtr.chic" ');
ok($arr, "query request to fetch values.input sent successfully");

$result= $arr->[0]->{'values.input'};
ok( defined($result) , " query to fetch values of input with date format (MM/DD/YYYY HH::MM::SS TZ) from Mongo successful ");

validate_results($arr->[0]->{'values.input'},4867);

# Testing Like operator

$arr= $query->run_query( query =>'get values.input between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC")  from tsdstest where intf like "ge-0/" ');
ok($arr, "query request to fetch values.input using like  sent successfully");

$result= $arr->[0]->{'values.input'};
ok( defined($result) , " query to fetch values of input using like operator executed successfully");

validate_results($arr->[0]->{'values.input'},4867);

# Testing not like operator

$arr= $query->run_query( query =>'get values.input between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC")  from tsdstest where intf not like "ge-0/" ');
ok($arr, "query request to fetch values.input using not like  sent successfully");

$result= $arr->[0]->{'values.input'};
ok( defined($result) , " query to fetch values of input using not like operator executed successfully");

# Testing Like, or , and, in,not in and Not like combinations
$arr= $query->run_query( query =>'get intf,node between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC")  from tsdstest where intf not like "interface" and intf != "ge-0/0/0"  or intf = null');
ok($arr, "query request to fetch values.input using not like , or combination sent successfully");
is( $arr->[0]->{'intf'},"xe-0/1/0.0","Interface value XE is validated");

$arr= $query->run_query( query =>'get values.input,intf,node between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node,intf  from tsdstest where intf not like "interface" and node="rtr.newy" ');
is( $arr->[0]->{'node'},"rtr.newy","Node value rtr.newy is validated");
is( $arr->[0]->{'intf'},"xe-0/1/0.0","Interface value xe is validated");

$arr= $query->run_query( query =>'get values.input,intf,node between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node,intf  from tsdstest where intf not like "interface" and (intf like "ge" or node = "rtr.chic") ');
is( $arr->[0]->{'intf'},"ge-0/0/0","Interface value ge-0/0/0 is validated");

$arr= $query->run_query( query =>'get values.input,intf,node between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node,intf  from tsdstest where intf like "interface" and node="rtr.newy" and intf in("interface3") ');
is( $arr->[0]->{'node'},"rtr.newy","Node value rtr.newy  using like , and and in combination is validated");
is( $arr->[0]->{'intf'},"interface3","Interface value returned by query using like , and and in combination is validated");

# No NOT IN used as of now

$arr= $query->run_query( query =>'get intf,node between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node,intf  from tsdstest where intf not like "interface" and node="rtr.chic" or node like "rtr.c" and intf in ("interface5","interface4","interface3","interface2","interface6","interface7","interface9","interface10","interface11")  or intf in ("interface2") ');

ok($arr,"query request with multiple combination of IN , or , Not in & and operator");

is($arr->[0]->{'intf'},"ge-0/0/0","Interface value returned by query with combination of operators( in,like,not like, or , and) is valid");
is($arr->[0]->{'node'},"rtr.chic","Node value returned by query with combination of operators( in,like,not like, or , and) is valid");

$arr= $query->run_query( query =>'get values.input,intf,node between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node,intf  from tsdstest where intf like "interface" and node="rtr.chic" and intf in("1","interface11") ');
is( $arr->[0]->{'node'},"rtr.chic","Node value rtr.chic is validated");
is( $arr->[0]->{'intf'},"interface11","Interface value returned by query is valid");

# Testing is Null operator

$arr= $query->run_query( query =>'get values.input between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC")  from tsdstest where end = null and intf like "ge-0/" ');
ok($arr, "query request to fetch values.input using like and NULL validation check  sent successfully");

$result= $arr->[0]->{'values.input'};
ok( defined($result) , " query to fetch values of input using not like and NULL operator check  executed successfully");

#validate_results($arr->[0]->{'values.input'},4867);


$arr= $query->run_query( query =>'get values.input,intf,node between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node,intf  from tsdstest where intf like "interface" and node="rtr.newy" and intf in("interface3") and node != null');
is( $arr->[0]->{'node'},"rtr.newy","Node value rtr.newy  using like , and,not null  and in combination is validated");
is( $arr->[0]->{'intf'},"interface3","Interface value returned by query using like ,not null  and and in combination is validated");


# Testing IN OPERATOR
$arr = $query->run_query( query =>'get values.input between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node from tsdstest where node in ("rtr.chic","rtr.newy") ');
ok($arr, "query request to fetch values.input using IN operator  validation check  sent successfully");

$result= $arr->[0]->{'values.input'};
ok( defined($result) , " query to fetch values of input using IN operator where condition executed successfully");

#validate_results($arr->[0]->{'values.input'},4867);

# Testing Greater than operator
$arr= $query->run_query( query =>'get values.input between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC")  from tsdstest where start >=0 and node = "rtr.chic" ');
ok($arr, " query request to fetch values.input using Greater Than operator ");

$result= $arr->[0]->{'values.input'};
ok( defined($result) , " query to fetch values of input using not like and NULL operator check  executed successfully");

# Testing != operator

$arr= $query->run_query( query => 'get values.output between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC")  from tsdstest where node != "rtr.newy" ');
ok($arr, " query request to fetch values.input using Greater Than operator ");

$result= $arr->[0]->{'values.output'};
ok( defined($result) , " query to fetch values of output using not like and NULL operator check  executed successfully");


# testing  < operator
$arr= $query->run_query( query =>'get values.input between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC")  from tsdstest where start < 48600 and node = "rtr.newy" ');
ok($arr, " query request to fetch values.input using less Than operator ");

$result= $arr->[0]->{'values.input'};
ok( defined($result) , " query to fetch values of input using less than  (<) operator check  executed successfully");


# testing between operator
#$arr= $query->run_query( query => 'get node between ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where node="rtr.chic" and start between(1000,100000) ');
#ok($arr, " query

#$arr= $query->run_query( query =>'get values.input between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC")  from tsdstest where start between (0,100) and intf="ge-0/0/0"');
#ok($arr, " query request to fetch values.input using between  operator sent successfully ");

#$result= $arr->[0]->{'values.input'};
#ok( defined($result) , " query to fetch values of input using between  executed successfully");


# testing multiple where operators
$arr= $query->run_query( query =>'get node  between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by node from tsdstest where (node="rtr.chic"  or node="rtr.newy" ) and intf="ge-0/0/0"');
ok($arr, " query request to fetch values.input using Multiple where conditions and By operator ");

$result= $arr->[0]->{'node'};
ok( defined($result) , "query executed successfully and able to retrieve node column");
is( $result, 'rtr.chic', " Node value validated succesfully");

$arr = $query->run_query( query => 'get intf,node,values.input  between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by intf,node,values.input  from tsdstest where (node="rtr.newy" or intf="ge-0/0/0") ordered by node');
ok($arr, " query request to fetch values.input using Multiple where conditions and multiple By operators test 2 ");

$result= $arr->[0]->{'node'};
ok( defined($result) , "query executed successfully and able to retrieve node column");
is( $result, 'rtr.chic', " Node First value validated succesfully");

$len = scalar @$arr -1;
$result= $arr->[$len]->{'node'};
is( $result, 'rtr.newy', " Node value validated succesfully");

# same as above query except ordered by node desc

$arr = $query->run_query( query => 'get intf,node,values.input  between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by intf,node,values.input  from tsdstest where (node="rtr.newy" or intf="ge-0/0/0") ordered by node desc');
ok($arr, " query request to fetch values.input using Multiple where conditions and multiple By operators test 2 ");

$result= $arr->[0]->{'node'};
ok( defined($result) , "query executed successfully and able to retrieve node column");
is( $result, 'rtr.newy', " Node First value validated succesfully");

$len = scalar @$arr -1;
$result= $arr->[$len]->{'node'};
is( $result, 'rtr.chic', " Node value validated succesfully");

# Compound Query
$arr = $query->run_query( query => 'get intf,node,values.input  between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by intf,node,values.input  from tsdstest where (node="rtr.newy" or intf="ge-0/0/0") ordered by node desc ,intf desc');
$len = scalar @$arr -1;

$result= $arr->[0]->{'node'};
is( $result, 'rtr.newy', " Node First value validated succesfully");

$result= $arr->[0]->{'intf'};
is( $result,'xe-0/1/0.0'," interface value returned is valid ");

$result= $arr->[$len]->{'node'};
is( $result, 'rtr.chic', " Node First value validated succesfully");

$result= $arr->[$len]->{'intf'};
is( $result,'ge-0/0/0'," interface value returned is valid ");

# Grouping by multiple fields
$arr = $query->run_query( query => 'get intf,node,values.input  between("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") by intf,node,values.input  from tsdstest where intf="interface5" ');
$len = scalar @$arr -1;

# Default sort order of data ?? Validate data using conditional statememt . It should be one of newyork or chicago node and interface "interface5" associated with it
$result= $arr->[0]->{'node'};

if( $result eq "rtr.newy") {
    is( $result,"rtr.newy"," Node value returned is valid for grouping data by multiple fields");
    $result= $arr->[0]->{'intf'};
    is( $result,"interface5","Interface value returned is valid for grouping data by multiple fields");
}
else{
    $result= $arr->[0]->{'node'};
    is( $result,"rtr.chic"," Last Node value returned is valid for grouping data by multiple fields");
    $result= $arr->[0]->{'intf'};
    is( $result,"interface5"," Last Interface value returned is valid for grouping data by multiple fields");
}

$result= $arr->[$len]->{'node'};
if( $result eq "rtr.newy") {
    is( $result,"rtr.newy"," Last Node value returned is valid for grouping data by multiple fields");
    $result= $arr->[$len]->{'intf'};
    is( $result,"interface5"," Last Interface value returned is valid for grouping data by multiple fields");
}
else{
    is( $result,"rtr.chic"," Last Node value returned is valid for grouping data by multiple fields");
    $result= $arr->[$len]->{'intf'};
    is( $result,"interface5"," Last Interface value returned is valid for grouping data by multiple fields");
}

# All Together
$arr= $query->run_query( query => 'get intf as Interface,node as NodeName between("01/01/1970","01/02/1970") by intf,NodeName  from tsdstest where (node="rtr.newy" or intf="ge-0/0/0") ordered by NodeName desc ');
$len = scalar @$arr -1;

$result= $arr->[0]->{'Interface'};
is($result,"interface11"," Interface column renamed succesfully and value returned correctly");

$result= $arr->[0]->{'NodeName'};
is($result,"rtr.newy"," NodeName column renamed succesfully and value returned correctly");

$result= $arr->[$len]->{'Interface'};
is($result,"ge-0/0/0"," Interface column renamed succesfully and last value returned correctly");

$result= $arr->[$len]->{'NodeName'};
is($result,"rtr.chic"," NodeName column renamed succesfully and last value returned correctly");


