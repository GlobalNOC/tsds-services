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


    print << 'EOF';

Indexes need to be added for the new aggregation framework in 1.5.0 to work properly. Until added the system will skip aggregating collections that do not have this index set, so it will not implode, but it will also not work. These indexes can take a possibly long time to add depending on size of the data and horsepower of the box.

For every database and every data* collection in that database (data, data_3600, etc), we will need to run the following. This will start a background index creation process which means that it will not block other things from going on. The mongodb session in which you start it will appear to block but other things will keep going. 
    db.$collection_name.ensureIndex({updated: 1, identifier: 1}, {background: 1});

To check on the status of index building from the mongo shell:
    db.currentOp().inprog.forEach(function(n){ if (n.msg && n.msg.match(/Index Build/)){ print("Duration: " + n.secs_running + " secs"); printjson(n.msg); } });

The reason this is not automated is because we want it to be a deliberate process. This will cause IO and CPU usage on the machine to rise since it needs to re-examine a lot of things. Rather than script add all the indexes at once, we want to make sure there is sufficient overhead and make it a deliberate process.

There are a few things to be aware of during index creation: https://docs.mongodb.org/manual/core/index-creation/ In particular that if the mongod process terminates, the background index will be resumed as a foreground index and thus will block anything else from occurring until finished.

EOF


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
