#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;

use GRNOC::Config;
use GRNOC::WebService::Client;
use MongoDB;
use GRNOC::CLI;

my $cli = GRNOC::CLI->new();

my $user = $cli->get_input("WebService Username");
my $pass = $cli->get_password("Password");

#my $tsds_url = 'https://io3.bldc.grnoc.iu.edu/tsds/services/metadata.cgi'; 
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

#my $tsds_client = GRNOC::WebService::Client->new(uid => $user, 
#                                                 passwd => $pass,
#                                                 url => $tsds_url);

my $admin_client = GRNOC::WebService::Client->new(uid => $user, 
                                                  passwd => $pass,
                                                  url => $admin_url);

#delete_fields();
#add_fields();

#           #       
#           #
# FUNCTIONS #
#           #
#           #

sub unset_old_fields {
    my $measurement_coll = shift;
    my $unset_field = shift; 
    my $return = $measurement_coll->update({}, {'$unset' => {"$unset_field" => 1}}, {'multiple' => 1});
    print Dumper($return);
}

sub remove_index{
    my $measurement_coll = shift;
    my $omission = shift;
    $measurement_coll->drop_index($omission);
}

sub delete_fields{
    my $db = $mongo->get_database('interface');
    my $measurement_coll = $db->get_collection('measurements');
     
    my @deletions = ('pop.owner', 'pop.hands_and_eyes', 'pop.role', 'pop.pop_id', 'circuit.circuit_id', 'network_id', 'entity.entity_id', 'node_id');
    
    foreach my $omission (@deletions){
        my $deleted = $admin_client->delete_meta_fields(measurement_type => "interface", name => $omission);
        
        if (defined $deleted->{error_text} and $deleted->{'error_text'} =~ m/does not exist/){
	    print "Skipping drop_index and unset_fields\n";
  	    print "$omission...." . Dumper($deleted);
            next;
	}
        
	print "Dropping index and unsetting the field...\n"; 
	
	# drop the index now
    my $om = $omission.'_1';
    remove_index($measurement_coll, $om);
        
	# unset the field now
    unset_old_fields($measurement_coll, $omission);
    
    }
}

sub add_fields{
    my @additions = ('service.entity_type', 'circuit.carrier_type', 'circuit.customer_type');
    foreach my $addtn (@additions){
        my $add = $admin_client->add_meta_field(measurement_type => 'interface', name => $addtn);
        print Dumper($add);
    }
}
