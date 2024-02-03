package GRNOC::TSDS::Upgrade::1_4_2;

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';

use GRNOC::TSDS::Install;
use GRNOC::TSDS::MongoDB;
use Tie::IxHash;
use Data::Dumper;

use constant PREVIOUS_VERSION => '1.4.1';

sub upgrade {

    my ( $self, $upgrade ) = @_;

    ### UPGRADE CODE GOES HERE ###

    my $mongo = $upgrade->mongo_root;

    my $tsds_mongo = GRNOC::TSDS::MongoDB->new(config_file => $upgrade->config_file(),
                                               privilege   => 'root');

    if (! $tsds_mongo){
        $upgrade->error("Unable to make TSDS MongoDB object");
        return;
    }
    if ($tsds_mongo->error()){
        $upgrade->error("Unable to make TSDS MongoDB object: ". $tsds_mongo->error());
        return;
    }

    # ISSUE=12319 create and shard the new temporary workspace
    print "Creating temp workspace...\n";
    $tsds_mongo->get_database("__tsds_temp_space", create => 1 )->run_command({"create" => "__workspace"});

    print "Sharding temp workspace database...\n";
    if (! $tsds_mongo->enable_sharding("__tsds_temp_space")){
        $upgrade->error("Error sharding temp space: " . $tsds_mongo->error());
        return;
    }

    print "Sharding temp workspace collection...\n";
    if (! $tsds_mongo->add_collection_shard("__tsds_temp_space", "__workspace", "{'_id': 1}")){
        $upgrade->error("Error sharding temp space collection: " . $tsds_mongo->error());
        return;
    }


    # ISSUE=12241
    # Add index on identifier in measurements
    my @all_databases = $mongo->database_names;
    foreach my $db_name (@all_databases){
        next if ($db_name eq 'admin' ||
                 $db_name eq 'config' ||
                 $db_name eq 'local');

        my $database = $mongo->get_database($db_name);

        my @all_collections = $database->collection_names;

        if ( grep( /^measurements$/, @all_collections) ) {
            print "Adding identifier index to measurements in $db_name\n";
            my $collection = $database->get_collection('measurements');
            $collection->ensure_index({identifier => 1});

            # ISSUE=12304 add missing start/end indexes in a few places
            $collection->ensure_index({start => 1});
            $collection->ensure_index({end => 1});
        }
    }


    ### END UPGRADE CODE ###

    return 1;
}

1;
