#!/usr/bin/perl

#--------------------------------------------------------------------
#----- GRNOC TSDS Push GlobalNOC WebService Wrapper
#-----
#----- Copyright(C) 2012 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $LastChangedBy: $
#----- $LastChangedRevision: $
#----- $LastChangedDate: $
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/www/push.cgi $
#----- $Id: push.cgi 38789 2015-08-21 16:42:38Z prattadi $
#-----
#----- This script is designed to be loaded by Apache + mod_perl and
#----- is just a wrapper to the GRNOC::TSDS::GWS::Push webservice
#----- library.
#--------------------------------------------------------------------
use strict;
use warnings;


use lib '../lib/';
use GRNOC::TSDS::GWS::Influx;
use GRNOC::TSDS::Util::ConfigChooser;

use FindBin;
use Data::Dumper;
use Mojolicious::Lite;


# needed for mod_perl
FindBin::again();

my $DEFAULT_CONFIG_FILE  = '/etc/grnoc/tsds/services/config.xml';
my $DEFAULT_LOGGING_FILE = '/etc/grnoc/tsds/services/logging.conf';
my $TESTING_CONFIG_FILE  = "$FindBin::Bin/../t/conf/tsds-services.xml";

my $config_file;
my $logging_file;
my $location;


# we may have already created the websvc object earlier in mod_perl env
$location = GRNOC::TSDS::Util::ConfigChooser->get_location($ENV{'REQUEST_URI'});
$config_file = $DEFAULT_CONFIG_FILE;
$logging_file = $location->{'logging_location'};

GRNOC::Log->new( config => $logging_file );


my $websvc = GRNOC::TSDS::GWS::Influx->new( config_file => $config_file );

post '/' => sub {
    my $c = shift;

    eval {
	my $result = $websvc->_add_influx_data(
	    {
		db => $c->param('db'),
		data => $c->req->text,
		user => $ENV{'REMOTE_USER'},
	    }
	);
	$c->render(
	    json => $result,
	    status => 204
	);
    };
    if ($@) {
	$c->render(json => {error => 1, error_text => "$@"});
    }
};

app->start;
