use strict;
use warnings;
use Test::More tests => 4;
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

# This tests running a series of subqueries that generate a very large number. In earlier versions
# of code this was mishandled and any math operations on it got reduced to undef.
my $response = $query->run_query( query => 'get values.total / 8 as values.bytes from (
get total * 9999999999999999 as values.total from (get input_sum + output_sum as total from (
get average(aggregate(values.input, 60, average)) as input_sum, average(aggregate(values.output, 60, average)) as output_sum between 
 ("01/01/1970 00:00:00 UTC","01/01/1970 13:31:00 UTC") from tsdstest where intf = "ge-0/0/0")
)
)' );

ok(defined $response, "got response back");
is(@$response, 1, "got 1 result");
is($response->[0]->{'values.bytes'}, 2.6528375e+20, 'got correct large number');
