#!/usr/bin/perl

use strict;
use warnings;

# One time script to add metafields for power
# Adds network, node_type, node_role, type, description, and alternate_intf fields

use Data::Dumper;

use GRNOC::Config;
use GRNOC::WebService::Client;
use MongoDB;
use GRNOC::CLI;

my $cli = GRNOC::CLI->new();

my $admin_url = $cli->get_input("TSDS (admin.cgi) WebService URL");

my $user = $cli->get_input("WebService Username");
my $pass = $cli->get_password("Password");

my $admin_client = GRNOC::WebService::Client->new(uid => $user,
                                                  passwd => $pass,
                                                  url => $admin_url,
                                                  realm => "https://idp.grnoc.iu.edu/idp/profile/SAML2/SOAP/ECP");

add_fields();

sub add_fields {
    # add new metadata fields
    my @additions = ('network', 'node_type', 'node_role', 'type', 'description', 'alternate_intf');

    foreach my $addtn (@additions) {
        my $add = $admin_client->add_meta_field(measurement_type => 'power', name => $addtn);
        die "$addtn: Couldn't add the field. Please check the URL, Username/Password.\n" if(!defined $add);
        if (defined $add->{'error_text'} and $add->{'error_text'} =~ m/already exists/){
            print "$addtn: Field already exists\n";
            next;
        }
        print "Added new metafield, $addtn\n";
    }
}
