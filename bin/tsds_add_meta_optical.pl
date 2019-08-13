#!/usr/bin/perl

use strict;
use warnings;

# Script to add metadata fields to optical data

use GRNOC::Config;
use GRNOC::WebService::Client;
use MongoDB;
use GRNOC::CLI;
use Data::Dumper;

my $cli = GRNOC::CLI->new();

my $admin_url = $cli->get_input("TSDS (admin.cgi) WebService URL");
my $user      = $cli->get_input("WebService Username");
my $pass      = $cli->get_password("Password");

my $client = GRNOC::WebService::Client->new(
    uid    => $user,
    passwd => $pass,
    url    => $admin_url,
    realm  => "https://idp.grnoc.iu.edu/idp/profile/SAML2/SOAP/ECP"
);

# Array of the metadata fields we want to add
my @metadata = (
    'alternate_intf',
    'circuit',
    'circuit.carrier',
    'circuit.carrier_type',
    'circuit.circuit_id',
    'circuit.customer',
    'circuit.customer_type',
    'circuit.description',
    'circuit.name',
    'circuit.owner',
    'circuit.role',
    'circuit.speed',
    'circuit.type',
    'contracted_bandwidth',
    'description',
    'entity',
    'entity.contracted_bandwidth',
    'entity.name',
    'entity.type',
    'interface_address',
    'interface_address.mask',
    'interface_address.type',
    'interface_address.value',
    'interface_id',
    'intf',
    'max_bandwidth',
    'network',
    'node',
    'node_management_address',
    'node_role',
    'node_type',
    'parent_interface',
    'pop',
    'pop.locality',
    'pop.name',
    'pop.type',
    'service',
    'service.contracted_bandwidth',
    'service.description',
    'service.entity',
    'service.entity_type',
    'service.name',
    'service.service_id',
    'service.type',
    'tag',
    'type'
);

# Hash of options for metadata fields with set options
my %options = (
    'circuit' => {
        array      => 1,
        classifier => 1
    },
    'circuit.name' => {
        ordinal => 1
    },
    'circuit.description' => {
        ordinal => 2
    },
    'description' => {
        ordinal => 3
    },
    'interface_address' => {
        array => 1
    },
    'intf' => {
        ordinal => 2
    },
    'node' => {
        ordinal => 1
    },
    'service' => {
        array      => 1,
        classifier => 1
    },
    'service.name' => {
        ordinal => 1
    },
    'service.description' => {
        ordinal => 2
    },
    'tag' => {
        array => 1
    }
);

# Add each metadata field in order
foreach my $field (@metadata) {

    my $array      = 0;
    my $ordinal    = 0;
    my $classifier = 0;

    if (exists $options{$field}) {
        if (exists $options{$field}{array}) {
            $array = $options{$field}{array};
        }
        if (exists $options{$field}{ordinal}) {
            $ordinal = $options{$field}{ordinal};
        }
        if (exists $options{$field}{classifier}) {
            $classifier = $options{$field}{classifier};
        }
    }

    # Add the field and capture the result for logging
    my $res = $client->add_meta_field(
        measurement_type => 'optical', 
        name             => $field,
        array            => $array,
        ordinal          => $ordinal,
        classifier       => $classifier
    );

    # Ensure there is a result or die
    if (defined $res) {

        # Log a status message
        unless (defined $res->{error_text}) {
            print("Successfully added $field to metadata fields for optical\n");
        }
        else { 
            print("Error: $res->{error_text}\n");
        }
    }
    else {
        die("Couldn't add $field to optical metadata. Check the URL, username, and password.\n");
    }
}
