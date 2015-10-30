use strict;
use warnings;

use Test::More tests => 17;

use GRNOC::Config;
use GRNOC::TSDS::DataService::Query;
use Data::Dumper;
use FindBin;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $bnf_file = "$FindBin::Bin/../conf/query_language.bnf";

my $query = GRNOC::TSDS::DataService::Query->new( config_file => $config_file,
                                                  bnf_file => $bnf_file );

ok($query, "query data service connected");

my $result = $query->run_query(query => "get type, start, end, identifier, text, node between(\"01/01/1970 00:00:00 UTC\", \"01/02/1970 00:00:00 UTC\") from tsdstest.event ordered by start, text");
ok($result, "tsdstest.event query completed successfully");

is(@$result, 48, "found 48 events");

my $first = $result->[0];
is($first->{'text'}, "This is the event at index 1 for node rtr.chic", "correct event text");
is($first->{'node'}->[0], "rtr.chic", "correct affected node");
is($first->{'start'}, 3600, "correct start time");
is($first->{'end'}, 3600 + 350, "correct end time");
is($first->{'type'}, 'alarm', 'correct type');
is($first->{'identifier'}, 'rtr.chic event 1', 'correct identifier' );


$result = $query->run_query(query => "get type, start, end, identifier, text, node between(\"01/01/1970 00:00:00 UTC\", \"01/02/1970 00:00:00 UTC\") from tsdstest.event where type = \"alarm\" and node = \"rtr.newy\" ordered by start, text");
ok($result, "tsdstest.event query completed successfully");

is(@$result, 24, "found 24 events");

$first = $result->[0];
is($first->{'text'}, "This is the event at index 1 for node rtr.newy", "correct event text");
is($first->{'node'}->[0], "rtr.newy", "correct affected node");
is($first->{'start'}, 3600, "correct start time");
is($first->{'end'}, 3600 + 350, "correct end time");
is($first->{'type'}, 'alarm', 'correct type');
is($first->{'identifier'}, 'rtr.newy event 1', 'correct identifier' );
