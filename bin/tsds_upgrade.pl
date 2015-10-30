#!/usr/bin/perl

use strict;
use warnings;

use Marpa::R2;
use GRNOC::TSDS::Upgrade;
use Getopt::Long;
use Data::Dumper;

### constants ###

use constant DEFAULT_CONFIG_FILE => '/etc/grnoc/tsds/services/config.xml';

### command line options ###

my $config_file = DEFAULT_CONFIG_FILE;
my $help;

GetOptions( 'config=s' => \$config_file,
            'help|h|?' => \$help ) or usage();

usage() if $help;

my $upgrader = GRNOC::TSDS::Upgrade->new( config_file => $config_file );

$upgrader->upgrade() or die( $upgrader->error );

sub usage {

    print "$0 [--config <file path>] [--help]\n";

    exit( 1 );
}
