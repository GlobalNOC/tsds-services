#!/usr/bin/perl

#--------------------------------------------------------------------
#----- GRNOC TSDS Search GlobalNOC WebService Wrapper
#-----
#----- Copyright(C) 2012 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $LastChangedBy: $
#----- $LastChangedRevision: $
#----- $LastChangedDate: $
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/www/search.cgi $
#----- $Id: search.cgi 34500 2014-12-09 15:27:29Z charmadu $
#-----
#----- This script is designed to be loaded by Apache + mod_perl and
#----- is just a wrapper to the GRNOC::TSDS::GWS::Search webservice
#----- library.
#--------------------------------------------------------------------

use strict;
use warnings;

use lib "../lib";
use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';

use GRNOC::TSDS::Config;
use GRNOC::TSDS::GWS::Search;
use GRNOC::TSDS::Util::ConfigChooser;

use FindBin;
use Data::Dumper;

# needed for mod_perl
FindBin::again();

my $DEFAULT_CONFIG_FILE  = '/etc/grnoc/tsds/services/config.xml';
my $DEFAULT_LOGGING_FILE = '/etc/grnoc/tsds/services/logging.conf';
my $TESTING_CONFIG_FILE  = "$FindBin::Bin/../t/conf/tsds-services.xml";


our $websvc;

my $config_file;
my $logging_file;
my $location;

# we may have already created the websvc object earlier in mod_perl env
if ( !defined( $websvc ) ) {

    if ( $ENV{'HTTP_HOST'} eq 'localhost:8529' ) {
        $config_file = $TESTING_CONFIG_FILE;
        $logging_file = $DEFAULT_LOGGING_FILE;
    }
    else {
        $location = GRNOC::TSDS::Util::ConfigChooser->get_location($ENV{'REQUEST_URI'});
        $config_file = $DEFAULT_CONFIG_FILE;
        $logging_file = $location->{'logging_location'};
    }

    GRNOC::Log->new( config => $logging_file );

    if (!-f $config_file) {
        $config_file = '';
    }
    my $config = new GRNOC::TSDS::Config(config_file => $config_file);
    $websvc = GRNOC::TSDS::GWS::Search->new(config => $config);
    $websvc->update_constraints_file($location->{'config_location'});
}

$location = GRNOC::TSDS::Util::ConfigChooser->get_location($ENV{'REQUEST_URI'});
$websvc->update_constraints_file($location->{'config_location'});

$websvc->handle_request();

