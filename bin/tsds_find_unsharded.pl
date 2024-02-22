#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5

use strict;
use warnings;

use Data::Dumper;
use Getopt::Long;

use GRNOC::Config;
use GRNOC::TSDS::MongoDB;


my $USAGE = "$0 --config <tsds services config file>";

my $config   = "";

GetOptions("config|c=s", \$config) or die $USAGE;

my $mongo = GRNOC::TSDS::MongoDB->new(config_file => $config,
                                      privilege   => 'root');

my $databases = $mongo->get_databases();

push(@$databases, "__tsds_temp_space");

foreach my $db_name (@$databases){
    print "\n\nChecking db \"$db_name\"\n";

    my $db       = $mongo->get_database($db_name);
    my $metadata = $db->get_collection('metadata')->find_one();

    # Skip non TSDS databases
    if (! $metadata && $db_name !~ /^__tsds/){
        print "  Not TSDS database, skipping\n";
        next;
    }

    my @collections = $db->collection_names;

    foreach my $collection (@collections){
        next unless ($collection =~ /(data|event|measurements|__work)/);
        next if ($collection =~ /metadata/);

        my $command = "db.getSiblingDB(\"$db_name\").getCollection(\"$collection\").getShardDistribution()";
        my $output  = $mongo->_execute_mongo($command);

        $output = $output->{'output'};

        if ($output =~ /not sharded/){
            print " !!! $db_name.$collection not sharded\n";
        }
        else {
            print " $db_name.$collection is good\n";
        }
    }
}

