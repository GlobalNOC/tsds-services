#!/usr/bin/perl

#--------------------------------------------------------------------
#----- GRNOC TSDS Installation/Bootstrap Script
#-----
#----- Copyright(C) 2015 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- This script is used to install/bootstrap a brand new instance
#----- the TSDS services.  It is simply a wrapper to the
#----- GRNOC::TSDS::Install object.
#--------------------------------------------------------------------

use strict;
use warnings;

use constant DEFAULT_CONFIG_FILE => '/etc/grnoc/tsds/services/config.xml';

use GRNOC::TSDS::Install;

use Getopt::Long;
use Data::Dumper;

# command line options
my $config_file = DEFAULT_CONFIG_FILE;
my $help;
my $testing_mode;

GetOptions( "config|c=s" => \$config_file,
            "testing-mode|t" => \$testing_mode,
            "help|h|?" => \$help );

# did they ask for help?
usage() if ( $help );

# create our installer object
my $installer = GRNOC::TSDS::Install->new( config_file => $config_file,
					   testing_mode => $testing_mode ) || exit( 1 );

# begin the installation process
my $success = $installer->install();

if ( !$success ) {

    print "Error: " . $installer->error() . "\n\n";
    exit( 1 );
}

else {

    exit( 0 );
}

sub usage {

    print "$0 [--config <config file>] [--help]\n";
    exit( 1 );
}
