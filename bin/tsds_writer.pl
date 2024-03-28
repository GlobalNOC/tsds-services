#!/usr/bin/perl

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';

use GRNOC::TSDS::Writer::Worker;

use Getopt::Long;
use Data::Dumper;

### constants ###

use constant DEFAULT_CONFIG_FILE => '/etc/grnoc/tsds/services/config.xml';
use constant DEFAULT_LOGGING_FILE => '/etc/grnoc/tsds/services/logging.conf';

### command line options ###

my $config = DEFAULT_CONFIG_FILE;
my $logging = DEFAULT_LOGGING_FILE;
my $queue;
my $help;

GetOptions( 'config=s' => \$config,
            'logging=s' => \$logging,
            'queue=s' => \$queue,
	    'help|h|?' => \$help );

# did they ask for help?
usage() if $help;

# create logger object
my $grnoc_log = GRNOC::Log->new( config => $logging );
my $logger = GRNOC::Log->get_logger();

# create config object
my $config_object = GRNOC::Config->new( config_file => $config, force_array => 0 );

# start/daemonize writer
my $worker = GRNOC::TSDS::Writer::Worker->new( config => $config_object, logger => $logger, queue => $queue );
$worker->start();

### helpers ###

sub usage {

    print "Usage: $0 [--config <file path>] [--logging <file path>] [--queue <queue name>]\n";

    exit( 1 );
}