#!/usr/bin/perl

use strict;
use warnings;

use FindBin;
use GRNOC::Log; 
use GRNOC::TSDS::Parser;

use Test::More tests => 82;

use Data::Dumper;

GRNOC::Log->new(config => "$FindBin::Bin/../conf/logging.conf");

my @basic_queries = (

    # with where clause
    'get name between("01/02/2014", "01/03/2014") from collection where x = "5"',
    'get name between("01/02/2014", "01/03/2014") from collection where x != "5"',
    'get name between("01/02/2014", "01/03/2014") from collection where x > 5',
    'get name between("01/02/2014", "01/03/2014") from collection where x >= 5',
    'get name between("01/02/2014", "01/03/2014") from collection where x < 5',
    'get name between("01/02/2014", "01/03/2014") from collection where x <= 5',
    'get name between("01/02/2014", "01/03/2014") from collection where x in ("5", "6", "7")',
    'get name between("01/02/2014", "01/03/2014") from collection where x between(5, 6)',
    'get name between("01/02/2014", "01/03/2014") from collection where x like "foobar"',    
    'get name between("01/02/2014", "01/03/2014") from collection where x not like "foobar"',    
    'get name between("01/02/2014", "01/03/2014") from collection where x in ("5", "6", "7")',    
    'get name between("01/02/2014", "01/03/2014") from collection where x = null',
    'get name between("01/02/2014", "01/03/2014") from collection',    

    # different date formats
    'get name between("01/02/2014 UTC", "01/03/2014") from collection where x > 5',
    'get name between("01/02/2014 04:03:20 UTC", "01/03/2014") from collection where x > 5',

    # with renaming
    'get name as foo between("01/02/2014", "01/03/2014") from collection where x > 7',  
    'get name as foo, test as bar between("01/02/2014", "01/03/2014") from collection where x > 7',  
    'get name as foo, test between("01/02/2014", "01/03/2014") from collection where x > 7',  

    # with multiple wheres
    'get name between("01/02/2014", "01/03/2014") from collection where x > 5 and x < 7',    
    'get name between("01/02/2014", "01/03/2014") from collection where x > 7 or x < 5',  

    # with grouping multiple wheres
    'get name between("01/02/2014", "01/03/2014") from collection where x > 7 and (y > 5 or y < 3)',  
    'get name between("01/02/2014", "01/03/2014") from collection where x > 7 or (y > 5 and y < 3)',  
    'get name between("01/02/2014", "01/03/2014") from collection where x > 7 or (y > 5 and y < 3) and z > 10',  
    'get name between("01/02/2014", "01/03/2014") from collection where (x > 7)',  
    'get name between("01/02/2014", "01/03/2014") from collection where (x > 7) or (y < 5)',  

    # with grouping by
    'get name between("01/02/2014", "01/03/2014") by meta.foo from collection where x > 7',  
    'get name between("01/02/2014", "01/03/2014") by meta.foo, meta.bar from collection where x > 7',

    # with ordering
    'get name between("01/02/2014", "01/03/2014") from collection where x > 7 ordered by meta.foo',  
    'get name between("01/02/2014", "01/03/2014") from collection where x > 7 ordered by meta.foo, meta.bar',  

    # aggregation functions
    'get average(output) between("01/02/2014", "01/03/2014") from collection where x > 5',
    'get percentile(output, 95) between("01/02/2014", "01/03/2014") from collection where x > 5',
    'get count(output) between("01/02/2014", "01/03/2014") from collection where x > 5',
    'get min(output) between("01/02/2014", "01/03/2014") from collection where x > 5',
    'get max(output) between("01/02/2014", "01/03/2014") from collection where x > 5',
    'get sum(output) between("01/02/2014", "01/03/2014") from collection where x > 5',
    'get histogram(output, 1) between("01/02/2014", "01/03/2014") from collection where x > 5',
    'get extrapolate(output, "01/02/2014") between("01/02/2014", "01/03/2014") from collection where x > 5',    
    'get extrapolate(output, 1000) between("01/02/2014", "01/03/2014") from collection where x > 5',    

    # symbols
    'get output, description between ("01/02/2014", "01/03/2014") from collection where description like "EDGE1<->ALMA-CORE1"',
    'get output, description between ("01/02/2014", "01/03/2014") from collection where description = "EDGE1<->ALMA-CORE1"',
    'get output, description between ("01/02/2014", "01/03/2014") from collection where description like "semicolon;"',
    'get output, description between ("01/02/2014", "01/03/2014") from collection where description like "brackets [] {}"',
    'get output, description between ("01/02/2014", "01/03/2014") from collection where description like "foo!"',
    'get output, description between ("01/02/2014", "01/03/2014") from collection where description like "a comment?#"',
    'get output, description between ("01/02/2014", "01/03/2014") from collection where description like "carot ^ percent % dollar $"',

    # having clause
    'get output, description, average(values.input) as avg between ("01/02/2014", "01/03/2014") from collection having avg > 50',
    'get output, description, average(values.input) as avg between ("01/02/2014", "01/03/2014") from collection having avg > 50 or (avg < 100 and avg > 10)',

    # all together
    'get name as foo between("01/02/2014", "01/03/2014") by meta.intf, meta.node from collection where x > 7 and (y < 3 or y > 8) and z > 10 ordered by foo, bar',  

    # chained together operators
    'get min(average(values.input)) between("01/02/2014", "01/03/2014") by meta.intf, meta.node from collection where x > 7 and (y < 3 or y > 8) and z > 10 ordered by foo, bar',  
    'get min(average(values.input)) as foo between("01/02/2014", "01/03/2014") by meta.intf, meta.node from collection where x > 7 and (y < 3 or y > 8) and z > 10 ordered by foo, bar',  
    'get min(average(values.input)) + 5  between("01/02/2014", "01/03/2014") by meta.intf, meta.node from collection where x > 7 and (y < 3 or y > 8) and z > 10 ordered by foo, bar',  
    'get min(average(values.input)) / 12.5 as bar between("01/02/2014", "01/03/2014") by meta.intf, meta.node from collection where x > 7 and (y < 3 or y > 8) and z > 10 ordered by foo, bar',  
    'get min(average(aggregate(values.input, 3600, average))) as foo between("01/02/2014", "01/03/2014") by meta.intf, meta.node from collection where x > 7 and (y < 3 or y > 8) and z > 10 ordered by foo, bar',  

    # by first()
    'get name as foo between("01/02/2014", "01/03/2014") by meta.node first(meta.intf) from collection where x > 7 and (y < 3 or y > 8) and z > 10 ordered by foo, bar',  
    'get name as foo between("01/02/2014", "01/03/2014") by meta.node first(meta.intf, meta.node) from collection where x > 7 and (y < 3 or y > 8) and z > 10 ordered by foo, bar',  

    # different between() modes
    'get name between(now-5m, now) by meta.node from collection where x = 4',
    'get name between("01/02/2014", now) by meta.node from collection where x = 4',
    'get name between("01/02/2014", now+1h) by meta.node from collection where x = 4',
    'get name between(1234567890, now+1mo) by meta.node from collection where x = 4',
    'get name between(1234567890, 1444567890) by meta.node from collection where x = 4',
    'get name between(now - 10s, now) by meta.node from collection where x = 4',
    'get name between(now - 10m, now) by meta.node from collection where x = 4',
    'get name between(now - 10h, now) by meta.node from collection where x = 4',
    'get name between(now - 10d, now) by meta.node from collection where x = 4',
    'get name between(now - 10w, now) by meta.node from collection where x = 4',
    'get name between(now - 10mo, now) by meta.node from collection where x = 4',
    'get name between(now - 10y, now) by meta.node from collection where x = 4',

    # aggregate function
    'get aggregate(values.input, 3600, sum) as foo between("01/02/2014", "01/03/2014") by meta.intf, meta.node from collection where x > 7 and (y < 3 or y > 8) and z > 10 ordered by foo, bar',  
    );


my $subquery = 'get name between("01/02/2014", "01/03/2014") from collection where x = "5"';

my @complex_queries = (
    # basic subquery
    "get name from ($subquery)",  
    "get name from ($subquery) ordered by foo",  
    "get name from ($subquery) with details",  
    "get name from ($subquery) with details ordered by foo",  
    "get name from ($subquery) where x < 5",  
    "get name from ($subquery) with details where x < 5",  
    "get name from ($subquery) with details where x < 5 ordered by foo",  

    # with group by
    "get name by meta.foo from ($subquery)",
    "get name by meta.foo from ($subquery) with details",
    "get name by meta.foo from ($subquery) with details ordered by foo",
    "get name by meta.foo, meta.bar from ($subquery) with details",
    "get name by meta.foo from ($subquery) where y < 5",
    "get name by meta.foo from ($subquery) with details where y < 5",
    "get name by meta.foo from ($subquery) with details where y < 5 and z > 10 ordered by foo",
    );


my $parser = GRNOC::TSDS::Parser->new(config_file => "$FindBin::Bin/conf/config.xml",
				      bnf_file => "$FindBin::Bin/../conf/query_language.bnf");

foreach my $query ((@basic_queries, @complex_queries)){

    my $result = $parser->tokenize($query);

    my $error = $parser->error();

    if (defined $error){
        #warn $error;
	BAIL_OUT("Error was: $error");
    }

    ok(! defined $error, $query);
}
