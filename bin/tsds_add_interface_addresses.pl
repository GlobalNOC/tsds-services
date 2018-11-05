#!/usr/bin/perl

use strict;
use warnings;

# One time script to add interface_address metafield
# Adds interface_address as an array with interface_address.type, interface_address.mask, and interface_address.value

use Data::Dumper;

use GRNOC::Config;
use GRNOC::WebService::Client;
use MongoDB;
use GRNOC::CLI;

my $cli = GRNOC::CLI->new();

my $user = $cli->get_input("WebService Username");
my $pass = $cli->get_password("Password");

my $admin_url = $cli->get_input("WebService URL");

my $config_file = "/etc/grnoc/tsds/services/config.xml";
my $config  = GRNOC::Config->new(config_file => $config_file);
my $root_user = $config->get('/config/mongo/root/@user')->[0];
my $root_pass = $config->get('/config/mongo/root/@password')->[0];
my $host    = $config->get('/config/mongo/@host')->[0];
my $port    = $config->get('/config/mongo/@port')->[0];

my $mongo = MongoDB::MongoClient->new(host => "$host:$port",
                                      username => $root_user,
                                      password => $root_pass);

my $admin_client = GRNOC::WebService::Client->new(uid => $user,
                                                  passwd => $pass,
                                                  url => $admin_url);

add_fields();

sub add_fields {
    # add interface_address array first
    my $interface_address = $admin_client->add_meta_field(measurement_type => 'interface', name => 'interface_address', array => 1);
    die "Couldn't add interface_address\n" . $interface_address if(!defined $interface_address); 

 
    my @additions = ('interface_address.type', 'interface_address.mask', 'interface_address.value');
    foreach my $addtn (@additions){
        my $add = $admin_client->add_meta_field(measurement_type => 'interface', name => $addtn);
        die "Couldn't add the field\n" if(!defined $add);
        if (defined $add->{'error_text'} and $add->{'error_text'} =~ m/already exists/){
            print "Field already exists\n";
            next;
	}
        print "Added new metafield, $addtn\n";       
    }
}
