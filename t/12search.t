use strict;
use warnings;
use Test::More tests => 8;
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
                        value_field_value    => [1000],
                        value_field_logic    => [">"],
                        value_field_function => ["average"],
			start_time           => 1,
			end_time             => 7200
    )->{'results'};

is(@$results, "0", "no results with average > 1000");


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
                        value_field_value    => ["11"],
			step                 => 1,
                        value_field_logic    => ["="],
                        value_field_function => ["percentile_95"],
			start_time           => 1,
			end_time             => 100
    )->{'results'};

is(@$results, "10", "10 results with percentile_95 >= blah");


