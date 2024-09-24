#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
#--------------------------------------------------------------------
#----- GRNOC TSDS Install/Bootstrap Library
#-----
#----- Copyright(C) 2015 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- This module is responsible for installating/bootstrapping a
#----- brand new instance of the TSDS backend services.  It is used
#----- by the tsds_install script to initialize the necessary MongoDB
#----- databases and collections.
#--------------------------------------------------------------------
package GRNOC::TSDS::Install;

use strict;
use warnings;

use GRNOC::CLI;

use GRNOC::TSDS;
use GRNOC::TSDS::MongoDB;
use GRNOC::TSDS::Config;
use GRNOC::TSDS::Constants;
use GRNOC::TSDS::DataService::MetaData;
use GRNOC::TSDS::DataService::Aggregation;

use JSON::XS;
use File::Slurp;

use Data::Dumper;

### constants ###
use constant INSTALL_DIR => "/usr/share/doc/grnoc/tsds/install/";

use constant ONE_HOUR => 60 * 60;
use constant ONE_DAY  => ONE_HOUR * 24;
use constant ONE_YEAR => ONE_DAY * 365;

use constant AGGREGATES => {'interface' => [{'name'     => 'one_hour',
					     'interval' => ONE_HOUR,
					     'meta'     => '{}',
					     'max_age'  => ONE_YEAR * 20,
					     'eval_position' => 10,
					     'values' => {'input' => {'hist_res' => 0.1,
								      'hist_min_width' => 10000000},
							  'output' => {'hist_res' => 0.1,
								       'hist_min_width' => 10000000},
							  'inerror' => {'hist_res' => 0.1,
									'hist_min_width' => 10000},
							  'outerror' => {'hist_res' => 0.1,
									 'hist_min_width' => 10000},
							  'inUcast' => {'hist_res' => 0.1,
									'hist_min_width' => 10000},
							  'outUcast' => {'hist_res' => 0.1,
									 'hist_min_width' => 10000},
							  'indiscard' => {'hist_res' => 0.1,
									  'hist_min_width' => 10000},
							  'outdiscard' => {'hist_res' => 0.1,
									   'hist_min_width' => 10000},
							  'status' => {'hist_res' => undef,
								       'hist_min_width' => undef}}},
					    
					    {'name'     => 'one_day',
					     'interval' => ONE_DAY,
					     'meta'     => '{}',
					     'max_age'  => ONE_YEAR * 20,
					     'eval_position' => 20,
					     'values' => {'input' => {'hist_res' => 0.1,
								      'hist_min_width' => 10000000},
							  'output' => {'hist_res' => 0.1,
								       'hist_min_width' => 10000000},
							  'inerror' => {'hist_res' => 0.1,
									'hist_min_width' => 10000},
							  'outerror' => {'hist_res' => 0.1,
									 'hist_min_width' => 10000},
							  'inUcast' => {'hist_res' => 0.1,
									'hist_min_width' => 10000},
							  'outUcast' => {'hist_res' => 0.1,
									 'hist_min_width' => 10000},
							  'indiscard' => {'hist_res' => 0.1,
									  'hist_min_width' => 10000},
							  'outdiscard' => {'hist_res' => 0.1,
									   'hist_min_width' => 10000},
							  'status' => {'hist_res' => undef,
								       'hist_min_width'=> undef}}}],

			    'tsdstest' => [{'name' => 'five_min',
					    'interval' => 300,
					    'last_run' => undef,
					    'meta' => '{}',
					    'max_age'  => ONE_YEAR * 20,
					    'eval_position' => 10,
					    'values' => {'input' => {'hist_res' => 0.1,
								     'hist_min_width' => 1000000000},
							 'output' => {'hist_res' => 0.1,
								      'hist_min_width' => 1000000000}}},
					   
					   {'name' => 'one_hour',
					    'interval' => 3600,
					    'last_run' => undef,
					    'meta' => '{}',
					    'max_age'  => ONE_YEAR * 20,
					    'eval_position' => 20,
					    'values' => {'input' => {'hist_res' => 0.1,
								     'hist_min_width' => 1000000000},
							 'output' => {'hist_res' => 0.1,
								      'hist_min_width'=> 1000000000}}},

					   {'name' => 'one_day',
					    'interval' => 86400,
					    'last_run' => undef,
					    'meta' => '{}',
					    'max_age'  => ONE_YEAR * 20,
					    'eval_position' => 30,
					    'values' => {'input' => {'hist_res' => 0.1,
								     'hist_min_width' => 1000000000},
							 'output' => {'hist_res' => 0.1,
								      'hist_min_width'=> 1000000000}}}],					   

			    'cpu' => [{'name' => 'one_hour',
				       'interval' => ONE_HOUR,
				       'meta' => '{}',
				       'max_age' => ONE_YEAR * 20,
				       'eval_position' => 10,
				       'values' => {'cpu' => {'hist_res' => 1,
							      'hist_min_width' => 1}}},
				      {'name' => 'one_day',
				       'interval' => ONE_DAY,
				       'meta' => '{}',
				       'max_age' => ONE_YEAR * 20,
				       'eval_position' => 20,
				       'values' => {'cpu' => {'hist_res' => 1,
							      'hist_min_width' => 1}}}],
			    'temperature' => [{'name' => 'one_hour',
					       'interval' => ONE_HOUR,
					       'meta' => '{}',
					       'max_age' => ONE_YEAR * 20,
					       'eval_position' => 10,
					       'values' => {'temp' => {'hist_res' => 1,
								       'hist_min_width' => 1}}},
					      {'name' => 'one_day',
					       'interval' => ONE_DAY,
					       'meta' => '{}',
					       'max_age' => ONE_YEAR * 20,
					       'eval_position' => 20,
					       'values' => {'temp' => {'hist_res' => 1,
								       'hist_min_width'=> 1}}}],
			    'power' => [{'name' => 'one_hour',
					 'interval' => ONE_HOUR,
					 'meta' => '{}',
					 'max_age' => ONE_YEAR * 20,
					 'eval_position' => 10,
					 'values' => {'status' => {'hist_res' => undef,
								   'hist_min_width' => undef},
						      'apppower' => {'hist_res' => undef,
								     'hist_min_width' => undef},
						      'actpower' => {'hist_res' => undef,
								     'hist_min_width' => undef},
						      'current' => {'hist_res' => undef,
								    'hist_min_width' => undef},
						      'pf' => {'hist_res' => undef,
							       'hist_min_width' => undef},
						      'voltage' => {'hist_res' => undef,
								    'hist_min_width' => undef}}},
					{'name' => 'one_day',
					 'interval' => ONE_DAY,
					 'meta' => '{}',
					 'max_age' => ONE_YEAR * 20,
					 'eval_position' => 20,
					 'values' => {'status' => {'hist_res' => undef,
								   'hist_min_width' => undef},
						      'apppower' => {'hist_res' => undef,
								     'hist_min_width' => undef},
						      'actpower' => {'hist_res' => undef,
								     'hist_min_width' => undef},
						      'current' => {'hist_res' => undef,
								    'hist_min_width' => undef},
						      'pf' => {'hist_res' => undef,
							       'hist_min_width' => undef},
						      'voltage' => {'hist_res' => undef,
								    'hist_min_width' => undef}}}]};
		      

### constructor ###

sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    my $self = {'config_file' => undef,
                'install_dir' => INSTALL_DIR,
                'error' => undef,
                'testing_mode' => 0,
                'databases' => [],
                @_};

    bless( $self, $class );

    # create and store CLI object
    $self->cli( GRNOC::CLI->new() );

    # create and store config object
    $self->config(new GRNOC::TSDS::Config(config_file => $self->{'config_file'}));

    # create and store MongoDB object
    $self->mongo_root(new GRNOC::TSDS::MongoDB(config => $self->config, privilege => 'root'));
    if ( !$self->mongo_root() ) {
        $self->error( 'An error occurred attempting to connect to MongoDB.' );
        return;
    }

    # create and store JSON object
    $self->json( JSON::XS->new() );

    return $self;
}

### getters/setters ###

sub cli {

    my ( $self, $cli ) = @_;

    $self->{'cli'} = $cli if ( defined( $cli ) );

    return $self->{'cli'};
}

sub config {

    my ( $self, $config ) = @_;

    $self->{'config'} = $config if ( defined( $config ) );

    return $self->{'config'};
}

sub mongo_root {

    my ( $self, $mongo ) = @_;

    $self->{'mongo_root'} = $mongo if ( defined( $mongo ) );

    return $self->{'mongo_root'};
}

sub metadata_ds {

    my ( $self, $metadata_ds ) = @_;

    $self->{'metadata_ds'} = $metadata_ds if ( defined( $metadata_ds ) );

    return $self->{'metadata_ds'};
}

sub aggregation_ds {

    my ( $self, $aggregation_ds ) = @_;

    $self->{'aggregation_ds'} = $aggregation_ds if ( defined( $aggregation_ds ) );

    return $self->{'aggregation_ds'};
}

sub json {

    my ( $self, $json ) = @_;

    $self->{'json'} = $json if ( defined( $json ) );

    return $self->{'json'};
}

sub error {

    my ( $self, $error ) = @_;

    $self->{'error'} = $error if ( defined( $error ) );

    return $self->{'error'};
}

### public methods ###

sub install {

    my ( $self ) = @_;

    # start out by clearing the screen completely
    $self->cli()->clear_screen();

    # lets print out a nice pretty banner
    $self->_print_banner();

    # variables that will hold user input
    my $sure;

    # make sure they are sure :)
    print "This will initialize the mongo database with the necessary databases and collections.\n\n";

    if ( $self->{'testing_mode'} ) {

        $sure = "test";
    }

    else {

        $sure = $self->cli()->get_input( 'Are you sure? [y/N]',
                                         default => 'N',
                                         required => 0,
                                         pattern => 'y|N' );

        # they weren't sure
        if ( $sure =~ /n/i ) {

            $self->error( 'Installation halted by user.');
            return;
        }
    }

    print "Initializing MongoDB bootstrap data...\n";

    $self->_create_users() or return;

    # Now that users have been created we can create the dataservices that actually use them. Doing
    # this step earlier when using auth means that the rw and ro connections would fail because
    # the users don't exist yet
    # create the metadata dataservice
    $self->metadata_ds( GRNOC::TSDS::DataService::MetaData->new( config_file => $self->{'config_file'} ) );
    # detect error connecting to database
    if ( !$self->metadata_ds() ) {
        $self->error( 'An error occurred attempting to create the metadata dataservice.' );
        return;
    }

    # create the aggregation dataservice
    $self->aggregation_ds( GRNOC::TSDS::DataService::Aggregation->new( config_file => $self->{'config_file'} ) );
    # detect error connecting to database
    if ( !$self->aggregation_ds() ) {
        $self->error( 'An error occurred attempting to create the aggregation dataservice.' );
        return;
    }

    
    if ( !$self->_create_shards() ) {
	$self->error( 'An error occurred attempting to create the shards.' );
	return;
    }

    if ( !$self->_create_databases() ) {

	$self->error( 'An error occurred attempting to create the databases.' );
	return;
    }

    # also make sure to set the version of the schema
    if ( !$self->_set_version() ) {

	$self->error( 'An error occurred attempting to set the version.' );
	return;
    }

    # installation was a success
    return 1;
}

### private methods ###

sub _print_banner {

    my ( $self ) = @_;

    # what version is this
    my $version = $GRNOC::TSDS::VERSION;

    # whats the current year
    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime();
    $year += 1900;

    # print the banner title
    $self->_print_title( "GlobalNOC TSDS Installer v$version - Copyright(C) $year The Trustees of Indiana University" );
}

sub _print_title {

    my ( $self, $title ) = @_;

    my $len = length( $title );

    print "$title\n";

    # print some dash characters underneath the title
    print '=' x $len, "\n\n";
}

sub _set_version {

    my ( $self ) = @_;

    my $version_db = $self->mongo_root()->get_database( 'tsds_version', create => 1 );
    if(!$version_db){
        $self->error("Coulen't create tsds_version database");
        return;
    }
    my $collection = $version_db->get_collection('tsds_version');
    if(!$collection){
        $self->error("Coulen't create tsds_version.tsds_version collection");
        return;
    }

    # upsert so we either insert or update accordingly
    $collection->update_one( {},
			     {'$set' => {'version' => $GRNOC::TSDS::VERSION}},
			     {'upsert' => 1} );
}

sub _create_users {
    my ( $self ) = @_;

    # first create a tsds role that includes special actions the tsds
    # users will need that are not included with the built in read and readWrite roles
    print "Creating role tsds\n";
    my $response_json = $self->mongo_root()->_execute_mongo(
        "db.getSiblingDB(\"admin\").createRole({role: \"tsds\", privileges: [{ resource: { cluster: true }, actions: [\"listDatabases\"]}], roles: []})"
    ) or return;

    if ( $response_json->{'ok'} ne '1') {
        # ignore error if its due to role already existing
        if ( $response_json->{'errmsg'} !~ /already exists/ ) {
            $self->error( "Error while adding role 'tsds': " . $response_json->{'errmsg'} );
            return;
        }
    }

    # now create readwrite and readonly users with access to all our 
    # base measurement databases and our meta databases
    foreach my $user (@{$self->mongo_root()->_get_tsds_users()}) { 
        my $username = $user->{'user'};
        my $password = $user->{'password'};
        print "Creating user $username\n";

        my $response_json = $self->mongo_root()->_execute_mongo(
            "db.getSiblingDB(\"admin\").createUser({user: \"$username\", pwd: \"$password\", roles: [\"tsds\"]})"
        ) or return;
        if ( $response_json->{'ok'} ne '1') {
            # ignore error if its due to user already existing
            if ( $response_json->{'errmsg'} !~ /already exists/ ) {
                $self->error( "Error while adding user $username: " . $response_json->{'errmsg'} );
                return;
            }
        }
    }

    return 1;
}

sub _create_shards {

    my ( $self ) = @_;

    $self->mongo_root()->add_shard();

    return 1;
}

sub _create_databases {

    my ( $self ) = @_;

    # create the empty reports database
    $self->mongo_root()->get_database("tsds_reports", create => 1 )->run_command({"create" => "reports"});

    # create the temp workspace database
    $self->mongo_root()->get_database("__tsds_temp_space", create => 1 )->run_command({"create" => "__workspace"});
    if (! $self->mongo_root()->enable_sharding("__tsds_temp_space")){
        $self->error("Error sharding temp space: " . $self->mongo_root()->error());
        return;
    }
    
    if (! $self->mongo_root()->add_collection_shard("__tsds_temp_space", "__workspace", "{'_id': 1}")){
        $self->error("Error sharding temp space collection: " . $self->mongo_root()->error());
        return;
    }

    # find all .json files we'll need to create databases for
    my $ret = opendir( my $fh, $self->{'install_dir'} );

    # detect error
    if ( !$ret ) {

      $self->error( "Unable to open directory " . $self->{'install_dir'} );
        return;
    }

    while ( my $file = readdir( $fh ) ) {

        next if ( $file !~ /\.json$/ );

        # determine database name from file name
        my ( $database_name ) = $file =~ /^(.+)\.json$/;

        # keep track of this database for later
        push( @{$self->{'databases'}}, $database_name );

        my $metadata = read_file( $self->{'install_dir'} . $file, err_mode => 'carp' );

        if ( !$metadata ) {

            $self->error( "Unable to read $file." );
            return;
        }

        # decode JSON string
        eval {
           $metadata = $self->json()->decode( $metadata );
        };

        if ( $@ ) {

            $self->error( "Unable to JSON decode $file: $@" );
            return;
        }

        print "Setting up $database_name\n";

        my $response_json;

        # pull out meta fields
        my $meta_fields = $metadata->{'meta_fields'};

        # determine what the required fields are
        my $required_meta_fields = [];

        foreach my $name (keys %$meta_fields){
            if($meta_fields->{$name}{'required'}){
                push(@$required_meta_fields, $name);
            }
        }

        #
        ## add the measurement type with the required fields
        my $res = $self->metadata_ds()->add_measurement_type(
            name => $database_name,
            label => $metadata->{'label'},
            required_meta_field => $required_meta_fields,
	    expire_after => $metadata->{'expire_after'}
        );
        if(!$res){
            $self->error( "Couldn't add measurement type $database_name: ".$self->metadata_ds->error());
            return;
        }

        #
        ## add optional meta_fields and add additional info to required meta_fields
        foreach my $name (keys %$meta_fields){
            my $field_info = $meta_fields->{$name};
            $res = $self->_add_meta_field( database => $database_name,
                                           name => $name,
                                           info => $field_info );
            return if !$res;
        }
  
        # Add the values        
        my $value_fields = $metadata->{'values'};

        # determine what the required fields are
        foreach my $name (keys %$value_fields){
            $self->metadata_ds()->add_measurement_type_value(
                measurement_type => $database_name,
                name             => $name,
                description      => $value_fields->{$name}{'description'},
                units            => $value_fields->{$name}{'units'},
                ordinal          => $value_fields->{$name}{'ordinal'},
            );
        }

        #
        ## add aggregates
        my $aggregates = AGGREGATES->{$database_name};
        if ( $aggregates ) {
            foreach my $agg ( @$aggregates ) {
                # add aggregation record
                $res = $self->aggregation_ds()->add_aggregation(
                    measurement_type => $database_name,
                    interval         => $agg->{'interval'},
                    meta             => $agg->{'meta'},
                    name             => $agg->{'name'},
                    values           => $agg->{'values'}
                );
                if(!$res){
                    $self->error( "Couldn't add aggregate $database_name, ".$agg->{'name'}." ".$self->aggregation_ds->error());
                    return;
                }
                # add expiration record
                $res = $self->aggregation_ds()->add_expiration(
                    measurement_type => $database_name,
                    interval         => $agg->{'interval'},
                    meta             => $agg->{'meta'},
                    name             => $agg->{'name'},
                    max_age          => $agg->{'max_age'}
                );
                if(!$res){
                    $self->error( "Couldn't add expiration $database_name, ".$agg->{'name'}." ".$self->aggregation_ds->error());
                    return;
                }
            }
        }
    }

    return 1;
}

sub _add_meta_field {

    my ( $self, %args ) = @_;

    my $database_name = $args{'database'};
    my $name = $args{'name'};
    my $field_info = $args{'info'};

    my $res;

    # if its a required field edit it since we alredy added it
    if($field_info->{'required'}){
        $res = $self->metadata_ds()->update_meta_fields( 
            name => $name,
            measurement_type => $database_name,
            array => $field_info->{'array'},
            ordinal => $field_info->{'ordinal'},
            classifier => $field_info->{'classifier'}
        );
        if(!$res){
            $self->error( "Couldn't update meta field $name ".$self->metadata_ds->error());
            return;
        }
    }
    # otherwise add the optional metadata
    else {

        $res = $self->metadata_ds()->add_meta_field( 
            name => $name,
            measurement_type => $database_name,
            array => $field_info->{'array'},
            ordinal => $field_info->{'ordinal'},
            classifier => $field_info->{'classifier'},
            search_weight => $field_info->{'search_weight'}
        );
        if(!$res){
            $self->error( "Couldn't add meta field $name ".$self->metadata_ds->error());
            return;
        }
    }

    # were there any subfields?
    if ( $field_info->{'fields'} ) {
        foreach my $subfield ( keys( %{$field_info->{'fields'}} ) ) {
            $res = $self->_add_meta_field(
                database => $database_name,
                name => "$name.$subfield",
                info => $field_info->{'fields'}{$subfield}
            );
            return if !$res;
        }
    }
    return 1;
}

1;
