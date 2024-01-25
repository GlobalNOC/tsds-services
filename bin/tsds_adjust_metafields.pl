#!/usr/bin/perl

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';
use lib './venv/lib/perl5';

# One time fix to make field adjustments
# Deletes fields from tsds, drops the corresponding indexes and unsets them in the db
# Following fields will be removed by this script - pop.owner, pop.hands_and_eyes, pop.pop_id, pop.role, ...
# ... entity.entity_id, node_id, network_id
# Adds metafields - service.entity_type, circuit.carrier_type, circuit.customer_type

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

delete_fields();
add_fields();

########################
#                      #
#       FUNCTIONS      # 
#                      #
######################## 

sub unset_old_fields {
    my $measurement_coll = shift;
    my $unset_field = shift; 
    my $return = eval { 
        $measurement_coll->update({"$unset_field" => {'$exists' => 1}}, {'$unset' => {"$unset_field" => 1}}, {'multiple' => 1});
    };
    if($@) {
        die "Error unsetting the field - $unset_field, " . $@;
    }
    print "Successfully unset - $unset_field\n";
}

sub remove_index {
    my $measurement_coll = shift;
    my $omission = shift;
    my $return = eval { 
        $measurement_coll->drop_index($omission);
    };
    if($@){
       die "Error Dropping the index - $omission, " . $@;
    }
    print "Successfully dropped the index for $omission\n";
}

sub delete_fields {
    my $db = $mongo->get_database('interface');
    my $measurement_coll = $db->get_collection('measurements');
     
    my @deletions = ('pop.owner', 'pop.hands_and_eyes', 'pop.role', 'pop.pop_id', 'network_id', 'entity.entity_id', 'node_id');
    
    foreach my $omission (@deletions) {
        my $deleted = $admin_client->delete_meta_fields(measurement_type => "interface", name => $omission);
        die "Couldn't delete the field\n" if(!defined $deleted);
    
        if (defined $deleted->{'error_text'} and $deleted->{'error_text'} =~ m/does not exist/) {
            print "Skipping drop_index and unset_fields\n";
            next;
        }
        print "Deleted metafield, $omission\n";
        print "Unsetting the field...\n";     

        # unset the field now
        unset_old_fields($measurement_coll, $omission);

        print "Dropping index...\n"; 

        # drop the index now
        my $om = $omission.'_1';
        remove_index($measurement_coll, $om);
        
    }
}

sub add_fields {
    my @additions = ('service.entity_type', 'circuit.carrier_type', 'circuit.customer_type');
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
