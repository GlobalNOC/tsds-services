use strict;
use warnings;
use Test::More tests => 3;
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
