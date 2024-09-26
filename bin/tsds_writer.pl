#!/usr/bin/perl

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';

use GRNOC::Log;
use GRNOC::TSDS::Config;
use GRNOC::TSDS::Writer::Worker;

use Getopt::Long;
use Data::Dumper;


use constant DEFAULT_CONFIG_FILE => '/etc/grnoc/tsds/services/config.xml';
use constant DEFAULT_LOGGING_FILE => '/etc/grnoc/tsds/services/logging.conf';


my $config = '';
my $logging = DEFAULT_LOGGING_FILE;
my $queue;
my $help;

GetOptions(
    'config:s'  => \$config,  # defaults to ''
    'logging=s' => \$logging,
    'help|h|?'  => \$help
);

usage() if $help;


my $grnoc_log = new GRNOC::Log(config => $logging);
my $logger = GRNOC::Log->get_logger();

my $config_object = new GRNOC::TSDS::Config(config_file => $config);

my $worker = new GRNOC::TSDS::Writer::Worker(
    config => $config_object,
    logger => $logger,
    queue => $queue
);
$worker->start();


sub usage {
    print "Usage: $0 [--config <file path>] [--logging <file path>] [--queue <queue name>]\n";
    exit 1;
}
