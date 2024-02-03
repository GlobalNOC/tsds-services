package GRNOC::TSDS::Upgrade::1_2_3;

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';

use GRNOC::TSDS::Install;
use GRNOC::TSDS::MongoDB;
use Data::Dumper;

use constant PREVIOUS_VERSION => '1.2.2';

sub upgrade {

    my ( $self, $upgrade ) = @_;

    ### UPGRADE CODE GOES HERE ###

    my $mongo = $upgrade->mongo_root;

    # ISSUE=11495
    my $database = $mongo->get_database( 'tsds_reports' );
    my $collection = $database->get_collection( 'reports' );

    # set all prior reports to be "global"
    $collection->update( {}, {'$set' => {'constraint_key' => "/tsds/services"}}, {'multiple' => 1} );

    # Set up users
    my $install = GRNOC::TSDS::Install->new(config_file => '/etc/grnoc/tsds/services/config.xml');
    $install->_create_users();

    # This is kind of goofy. We're going to assume everything in Mongo is currently
    # under the domain of TSDS. This is a little risky but this early on in TSDS's life
    # likely not overly so.
    my $tsds_mongo = GRNOC::TSDS::MongoDB->new(privilege   => 'root',
					       config_file => '/etc/grnoc/tsds/services/config.xml' );


    # We're using the raw driver to grab all of the database names since it
    # should be using the root user. Then we're going to grant the TSDS
    # app users access to each of these databases, skipping the 3 default ones
    my @all_databases = $mongo->database_names;
    foreach my $db_name (@all_databases){
        next if ($db_name eq 'admin' ||
                 $db_name eq 'config' ||
                 $db_name eq 'local');

        print "Adding permissions for $db_name\n";

        # This actually grants access
        $tsds_mongo->get_database($db_name, create => 1);
    }

    # If there are no more tables left in a database mongo seems to garbage collect it, which 
    # then prevents it from being used later since a non-root user would be trying to create it.
    # The Install.pm script does this so we're mirroring it here - create a dummy table
    # inside the temp database so that there is always something.
    $tsds_mongo->get_database("__tsds_temp_space", create => 1)->run_command({"create" => "dummy"});


    ### END UPGRADE CODE ###

    return 1;
}

1;
