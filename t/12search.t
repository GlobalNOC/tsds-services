use strict;
use warnings;
use Test::More tests => 37;
use GRNOC::Config;
use GRNOC::Log;
use GRNOC::TSDS::DataService::Search;
use Data::Dumper;
use FindBin;

my $config_file  = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";
my $bnf_file = "$FindBin::Bin/../conf/query_language.bnf";

GRNOC::Log->new( config => $logging_file );

# start up searchd if it isn't already
system( 'sudo /etc/init.d/searchd start' );
sleep( 2 );

my $ds = GRNOC::TSDS::DataService::Search->new( config_file => $config_file,
						bnf_file => $bnf_file );

ok( $ds, "search data service connected" );


my $results = $ds->search( search => 'xe-0/1/0.0',
			   measurement_type => ['tsdstest'] )->{'results'};

is( @$results, 1, "1 search result for xe-0/1/0.0" );

$results = $ds->search( search => 'rtr.chic ge-0/0/0',
			measurement_type => ['tsdstest'] )->{'results'};

is( @$results, 1, "1 search result for rtr.chic ge-0/0/0" );

$results = $ds->search( search               => "rtr.chic",			
                        value_field_name     => ["input"],
                        value_field_value    => [1000],
                        value_field_logic    => [">"],
                        value_field_function => ["average"]);


like($ds->error(), qr/measurement_type with an undefined search term or when searching by values/, "failed to search by value when no measurement type set");

$results = $ds->search( search               => "rtr.chic",		
			measurement_type     => ['tsdstest'],
                        value_field_name     => ["input"],
                        value_field_value    => [1000000000],
                        value_field_logic    => [">"],
                        value_field_function => ["average"],
			start_time           => 1,
			end_time             => 7200
    )->{'results'};

is(@$results, "0", "no results with average > 1000000000");


$results = $ds->search( search               => "rtr.chic",		
			measurement_type     => ['tsdstest'],
                        value_field_name     => ["input"],
                        value_field_value    => [720.5],
                        value_field_logic    => [">="],
                        value_field_function => ["max"],
			start_time           => 1,
			end_time             => 7200
    )->{'results'};

is(@$results, "10", "10 results with max >= 720.5");


$results = $ds->search( search               => "rtr.chic",		
			measurement_type     => ['tsdstest'],
                        value_field_name     => ["input"],
                        value_field_value    => [2],
			step                 => 1,
                        value_field_logic    => [">="],
                        value_field_function => ["min"],
			start_time           => 1,
			end_time             => 60
    )->{'results'};

is(@$results, "10", "10 results with min >= 2");


$results = $ds->search( search               => "rtr.chic",		
			measurement_type     => ['tsdstest'],
                        value_field_name     => ["input"],
                        value_field_value    => ["129696"],
			step                 => 1,
                        value_field_logic    => ["="],
                        value_field_function => ["percentile_95"],
			start_time           => 1,
			end_time             => 1000
    )->{'results'};

is(@$results, "1", "1 result with percentile_95 = 129696");


# Ensure ordering by value still works in a single measurement type
# when there are no meta fields, ie no where clause
$results = $ds->search( search               => undef,		
                        measurement_type     => ['tsdstest'],
                        start_time           => 1,
                        end_time             => 7200,
                        order_by             => ["value_1"]
    );

is($results->{'total'}, 20, "20 results reported in total");
$results = $results->{'results'};
is(@$results, 20, "got all 20 results back");
is(_get_value("input", $results->[0])->{'aggregate'}, 361.25, "got first ordered value");
is(_get_value("input", $results->[1])->{'aggregate'}, 9001.25, "got second ordered value");
is(_get_value("input", $results->[19])->{'aggregate'}, 164521.25, "got last ordered value");

# Same as above but descending order
$results = $ds->search( search               => undef,		
                        measurement_type     => ['tsdstest'],
                        start_time           => 1,
                        end_time             => 7200,
                        order_by             => ["value_1"],
                        order                => "desc"
    );

is($results->{'total'}, 20, "20 results reported in total");
$results = $results->{'results'};
is(@$results, 20, "got all 20 results back");
is(_get_value("input", $results->[19])->{'aggregate'}, 361.25, "got last ordered value");
is(_get_value("input", $results->[18])->{'aggregate'}, 9001.25, "got second to last ordered value");
is(_get_value("input", $results->[0])->{'aggregate'}, 164521.25, "got first ordered value");


# Ensure "having" still works in a single measurement type
# where there are no meta fields, ie no where clause
$results = $ds->search( search               => undef,		
                        measurement_type     => ['tsdstest'],
                        start_time           => 1,
                        end_time             => 7200,
                        value_field_name     => ["input"],
                        value_field_value    => [12000],
			step                 => 1,
                        value_field_logic    => [">="],
                        value_field_function => ["min"]
    );

is($results->{'total'}, 18, "18 results reported in total");
$results = $results->{'results'};
is(@$results, 18, "got all 18 results back");
is(_get_value("input", $results->[0])->{'aggregate'}, 164521, "got value");


# Ensuring that search while doing a limit/offset and
# an order on value fields actually orders correctly on the
# whole dataset instead of just the limit/offset chunk
$results = $ds->search( search               => undef,		
                        measurement_type     => ['tsdstest'],
                        start_time           => 1,
                        end_time             => 7200,
                        order_by             => ["value_1"],
                        order                => "asc",
                        limit                => 2
    );

is($results->{'total'}, 20, "20 results reported in total");
$results = $results->{'results'};
is(@$results, 2, "got the limited 2 results back");
is(_get_value("input", $results->[0])->{'aggregate'}, 361.25, "got first ordered value");
is(_get_value("input", $results->[1])->{'aggregate'}, 9001.25, "got second ordered value");


# Same as above but now with an offset to verify we paged correctly
$results = $ds->search( search               => undef,		
                        measurement_type     => ['tsdstest'],
                        start_time           => 1,
                        end_time             => 7200,
                        order_by             => ["value_1"],
                        order                => "asc",
                        limit                => 2,
                        offset               => 2
    );

is($results->{'total'}, 20, "20 results reported in total");
$results = $results->{'results'};
is(@$results, 2, "got the limited 2 results back");
is(_get_value("input", $results->[0])->{'aggregate'}, 17641.25, "got third ordered value");
is(_get_value("input", $results->[1])->{'aggregate'}, 26281.25, "got fourth ordered value");



# Ensuring that search while doing a limit/offset and
# a having clause actually filters correctly on the
# whole dataset instead of just the limit/offset chunk
$results = $ds->search( search               => undef,		
                        measurement_type     => ['tsdstest'],
                        start_time           => 1,
                        end_time             => 7200,
                        value_field_name     => ["input"],
                        value_field_value    => [12000],
			step                 => 1,
                        value_field_logic    => [">="],
                        value_field_function => ["min"],
                        limit                => 2,
                        offset               => 0
    );

is($results->{'total'}, 18, "18 results reported in total");
$results = $results->{'results'};
is(@$results, 2, "got all 2 results back");
is(_get_value("input", $results->[0])->{'aggregate'}, 164521, "got value");
is(_get_value("input", $results->[1])->{'aggregate'}, 138601, "got value");


# Same as above with but offset
$results = $ds->search( search               => undef,		
                        measurement_type     => ['tsdstest'],
                        start_time           => 1,
                        end_time             => 7200,
                        value_field_name     => ["input"],
                        value_field_value    => [12000],
			step                 => 1,
                        value_field_logic    => [">="],
                        value_field_function => ["min"],
                        limit                => 2,
                        offset               => 2
    );

is($results->{'total'}, 18, "18 results reported in total");
$results = $results->{'results'};
is(@$results, 2, "got all 2 results back");
is(_get_value("input", $results->[0])->{'aggregate'}, 43561, "got value");
is(_get_value("input", $results->[1])->{'aggregate'}, 60841, "got value");



sub _get_value {
    my $name = shift;
    my $data = shift;

    foreach my $thing (@{$data->{'values'}}){
        if ($thing->{'name'} eq $name){
            return $thing->{'value'};
        }
    }
}
