#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;

use GRNOC::Config;
use GRNOC::WebService::Client;
use MongoDB;

my $tsds_url = 'https://io3.bldc.grnoc.iu.edu/tsds/services/metadata.cgi'; 
my $user;
my $pass;
my $admin_url = 'https://io3.bldc.grnoc.iu.edu/tsds/services/admin.cgi';

my $config_file = "/etc/grnoc/tsds/services/config.xml";
my $config  = GRNOC::Config->new(config_file => $config_file);
my $root_user = $config->get('/config/mongo/root/@user')->[0];
my $root_pass = $config->get('/config/mongo/root/@password')->[0];
my $host    = $config->get('/config/mongo/@host')->[0];
my $port    = $config->get('/config/mongo/@port')->[0];

my $mongo = MongoDB::MongoClient->new(host => "$host:$port",
                                      username => $root_user,
                                      password => $root_pass);

my $tsds_client = GRNOC::WebService::Client->new(uid => $user, 
                                                 passwd => $pass,
                                                 url => $tsds_url);

my $admin_client = GRNOC::WebService::Client->new(uid => $user, 
                                                  passwd => $pass,
                                                  url => $admin_url);

remove_index();

sub remove_index{
    my $db = $mongo->get_database('interface');
    my $measurement_coll = $db->get_collection('measurements');
    $measurement_coll->drop_index('pop.pop_id_1');
}

sub delete_fields{
    my $metafields = $tsds_client->get_meta_fields(measurement_type => "interface");

    my @deletions = ('pop.owner', 'pop.hands_and_eyes', 'pop.role', 'pop.pop_id', 'circuit.circuit_id', 'interface_id', 'network_id', 'entity.entity_id', 'node_id');


    foreach my $omission (@deletions){
        my $deleted = $admin_client->delete_meta_fields(measurement_type => "interface", name => $omission);
        # drop index now
        #
        print Dumper($deleted);
    }
}

sub add_fields{
    my @additions = ('service.service_entity_type', 'circuit.carrier_type', 'circuit.customer_type');
    foreach my $addtn (@additions){
        my $add = $admin_client->add_meta_field(measurement_type => 'interface', name => $addtn);
        print Dumper($add);
    }
}
