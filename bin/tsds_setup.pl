#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5

use strict;
use warnings;

use Marpa::R2;
use Getopt::Long;
use Data::Dumper;
use GRNOC::TSDS::Setup;

### constants ###

use constant DEFAULT_CONFIG_FILE => '/etc/grnoc/tsds/services/config.xml';

### command line options ###

my $config_file = DEFAULT_CONFIG_FILE;
my $help;
my $noconfirm = 0;

GetOptions( 'config=s'  => \$config_file,
    'noconfirm' => \$noconfirm,
    'help|h|?'  => \$help ) or usage();

usage() if $help;

my $installer = GRNOC::TSDS::Setup->new( config_file => $config_file,
    unattended  => $noconfirm);

$installer->setup() or die( $installer->error );

sub usage {

    print "$0 [--config <file path>] [--help]\n";

    exit( 1 );
}

