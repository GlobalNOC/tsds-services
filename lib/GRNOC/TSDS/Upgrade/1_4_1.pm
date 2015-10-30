package GRNOC::TSDS::Upgrade::1_4_1;

use strict;
use warnings;

use GRNOC::TSDS::Install;
use GRNOC::TSDS::MongoDB;
use Tie::IxHash;
use Data::Dumper;

use constant PREVIOUS_VERSION => '1.4.0';

sub upgrade {

    my ( $self, $upgrade ) = @_;

    ### UPGRADE CODE GOES HERE ###

    my $mongo = $upgrade->mongo_root;

    # ISSUE=11369

    # Looping through all the measurements in measurement collection of all the databases
    # Add index to last_updated and set the start value to it
    my @all_databases = $mongo->database_names;
    foreach my $db_name (@all_databases){
        next if ($db_name eq 'admin' ||
                 $db_name eq 'config' ||
                 $db_name eq 'local');


        my $database = $mongo->get_database($db_name);

        my @all_collections = $database->collection_names;

        if ( grep( /^measurements$/, @all_collections) ) {
            print "Adding last_updated to measurements in $db_name\n";
            my $collection = $database->get_collection('measurements');
            $collection->ensure_index({last_updated => 1});
            my $cursor = $collection->find({});
            while (my $doc = $cursor->next()) {
                my $query = Tie::IxHash->new( 'identifier' => $doc->{'identifier'},
                                              'start' => $doc->{'start'},
                                              'end' => $doc->{'end'} );

                $collection->update( $query, {'$set' => {'last_updated' => $doc->{'start'}}} );
            }
        }
    }

    ### END UPGRADE CODE ###

    return 1;
}

1;
