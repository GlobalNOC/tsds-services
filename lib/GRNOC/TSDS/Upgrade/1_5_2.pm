package GRNOC::TSDS::Upgrade::1_5_2;

use strict;
use warnings;

use GRNOC::TSDS::Install;
use GRNOC::TSDS::MongoDB;
use Tie::IxHash;
use Data::Dumper;

use constant PREVIOUS_VERSION => '1.5.1';

sub upgrade {

    my ( $self, $upgrade ) = @_;

    ### UPGRADE CODE GOES HERE ###

    my $mongo = $upgrade->mongo_root;

    my $metadata = GRNOC::TSDS::DataService::MetaData->new(config_file => $upgrade->config_file, 
							   privilege   => "root");
    
    # These were missing as documented meta fields which the meta manager
    # had been setting, adding these properly now in upgrade script. New install
    # will have them as well
    $metadata->add_meta_field(measurement_type => "interface",
			      name             => "pop.pop_id",
			      classifier       => undef,
			      ordinal          => undef,
			      array            => 0,
			      search_weight    => undef);

    $metadata->add_meta_field(measurement_type => "interface",
			      name             => "pop.owner",
			      classifier       => undef,
			      ordinal          => undef,
			      array            => 0,
			      search_weight    => undef);

    return 1;
}

1;
