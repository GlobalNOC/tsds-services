package GRNOC::TSDS::Upgrade::1_4_2;

use strict;
use warnings;

use GRNOC::TSDS::Install;
use GRNOC::TSDS::MongoDB;
use Tie::IxHash;
use Data::Dumper;

use constant PREVIOUS_VERSION => '1.4.1';

sub upgrade {

    my ( $self, $upgrade ) = @_;

    ### UPGRADE CODE GOES HERE ###

    my $mongo = $upgrade->mongo_root;

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
