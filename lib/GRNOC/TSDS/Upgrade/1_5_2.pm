package GRNOC::TSDS::Upgrade::1_5_2;

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';

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
    print "Adding missing indexes to interface...\n";
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

    my @db_names = $mongo->database_names;
    foreach my $db_name (@db_names){
	my $metadata = $mongo->get_database($db_name)->get_collection('metadata')->find_one();

	next if (! $metadata);

	print "Adding expire attribute to $db_name metadata\n";	
	$mongo->get_database($db_name)->get_collection('metadata')->update({}, {'$set' => {'expire_after' => 86400 * 2}});
    }

    return 1;
}

1;
