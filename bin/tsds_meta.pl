#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
use strict;
use warnings;

use Data::Dumper;
use Getopt::Long;

use GRNOC::TSDS::MetaStats;

### constants ###                                                                                                               

use constant DEFAULT_CONFIG_FILE => '/etc/grnoc/tsds/services/meta_config.xml';
use constant DEFAULT_LOGGING_FILE => '/etc/grnoc/tsds/services/logging.conf';

### command line options ###                                                                                                    

my $config = DEFAULT_CONFIG_FILE;
my $logging = DEFAULT_LOGGING_FILE;
my $nofork;
my $help;

GetOptions( 'config=s' => \$config,
            'logging=s' => \$logging,
            'nofork' => \$nofork,
            'help|h|?' => \$help );

# did they ask for help?                                                                                                        
usage() if $help;

my $stats = GRNOC::TSDS::MetaStats->new( config_file => $config,
                        logging_file => $logging,
			daemonize => 0);



$stats->start();

### helpers ###                                                                                                                 

sub usage {

    print "Usage: $0 [--config <file path>] [--logging <file path>] [--nofork]\n";

    exit( 1 );
}
