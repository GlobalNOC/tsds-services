#!/usr/bin/perl

use strict;
use warnings;

use GRNOC::Log;

use GRNOC::TSDS::MeasurementDecommer;

use Getopt::Long;
use Data::Dumper;

use constant DEFAULT_CONFIG  => '/etc/grnoc/tsds/services/config.xml';
use constant DEFAULT_LOGGING => '/etc/grnoc/tsds/services/logging.conf';
use constant USAGE => "$0: [--config | -c <config file>] [--logging | -l <logging config file>] [--help | -h]";

my $config_file = DEFAULT_CONFIG;
my $log_config  = DEFAULT_LOGGING;
my $help        = 0;

GetOptions(
    'config|c=s'    => \$config_file,
    'logging|l=s'   => \$log_config,
    'help|h|?'      => \$help,
    ) or die USAGE;

if ($help){
    print USAGE . "\n";
    exit(1);
}

if (! $config_file || ! -e $config_file){
    print "Missing or invalid config file: $config_file\n";
    print USAGE . "\n";
    exit(1);
}

GRNOC::Log->new(config => $log_config);
my $decommer = GRNOC::TSDS::MeasurementDecommer->new(config_file => $config_file);

my $results = $decommer->decom_metadata();

print "Decom results:\n";
foreach my $db_name (keys %$results){
    print "Database: $db_name  Num Decoms: $results->{$db_name}\n";
}

# all done
exit( 0 );
