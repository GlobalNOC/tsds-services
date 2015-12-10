package GRNOC::TSDS::Upgrade::1_5_0;

use strict;
use warnings;

use GRNOC::TSDS::Install;
use GRNOC::TSDS::MongoDB;
use Tie::IxHash;
use Data::Dumper;

use constant PREVIOUS_VERSION => '1.4.2';

sub upgrade {

    my ( $self, $upgrade ) = @_;

    ### UPGRADE CODE GOES HERE ###

    my $mongo = $upgrade->mongo_root;

    # ISSUE=12566 fix missing eval_position in aggregate and expire docs
    my @all_databases = $mongo->database_names;

    foreach my $db_name ( @all_databases ) {

        next if ($db_name eq 'admin' ||
                 $db_name eq 'config' ||
                 $db_name eq 'local');

        my $database = $mongo->get_database($db_name);

        my @all_collections = $database->collection_names;

        if ( grep( /^aggregate$/, @all_collections) ) {

            print "Fixing missing eval_position from aggregate in $db_name\n";

            my $collection = $database->get_collection( 'aggregate' );
	    $self->_fix_eval_positions( $collection );	    
        }

	if ( grep( /^expire$/, @all_collections ) ) {

	    print "Fixing missing eval_position from expire in $db_name\n";

	    my $collection = $database->get_collection( 'expire' );
	    $self->_fix_eval_positions( $collection );
	}
    }

    ### END UPGRADE CODE ###

    return 1;
}

sub _fix_eval_positions {

    my ( $self, $collection ) = @_;

    my @docs = $collection->find( {} )->all;

    my $highest_eval_position = 0;

    foreach my $doc ( @docs ) {

	my $eval_position = $doc->{'eval_position'};

	$highest_eval_position = $eval_position if ( $eval_position && $eval_position > $highest_eval_position );
    }
    
    my $last_eval_position = $highest_eval_position;
    
    foreach my $doc ( @docs ) {

	my $id = $doc->{'_id'};
	my $eval_position = $doc->{'eval_position'};

	next if ( $eval_position );
	
	$eval_position = $last_eval_position + 10;
	    
	$collection->update( {'_id' => $id}, {'$set' => {'eval_position' => $eval_position}} );
	
	$last_eval_position = $eval_position;
    }
}

1;
