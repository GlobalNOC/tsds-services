
package GRNOC::TSDS::MongoDB;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';
use lib './venv/lib/perl5';

use strict;
use warnings;

use GRNOC::Log;
use GRNOC::Config;
use GRNOC::TSDS::Constants;

use Storable qw( dclone );
use MongoDB;
use JSON;
use Sys::Hostname;

use Data::Dumper;

our $DATA_SHARDING  = "{'identifier': 1, 'start': 1, 'end': 1}";

my %singleton;

sub new {
    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    my $self = {	
        @_
    };

    bless( $self, $class );

    my $privilege;
    if(!defined($self->{'privilege'})){
        $self->error('You must specify the privilege you want mongo to connect with (root, ro, rw)');
        return;
    }
    elsif($self->{'privilege'} eq 'root' ){
        $privilege = 'root';	
    }elsif($self->{'privilege'} eq 'ro' ){
        $privilege = 'readonly';
    }elsif($self->{'privilege'} eq 'rw' ){
        $privilege = 'readwrite';
    }else {
        $self->error('You must specify the privilege you want mongo to connect with (root, ro, rw)');
        return;
    }    

    return $singleton{$privilege} if (defined $singleton{$privilege});
    $singleton{$privilege} = $self;

    my $config = GRNOC::Config->new(
        config_file => $self->{'config_file'},
        force_array => 0 
    );

    $self->{'config'} = $config;

    my $host = $self->{'config'}->get( '/config/mongo/@host' );
    my $port = $self->{'config'}->get( '/config/mongo/@port' );

    # store our configurably igrnored databases
    $self->{'config'}->{'force_array'} = 1;
    my $ignore_databases = $self->{'config'}->get( '/config/ignore-databases/database' );
    $self->{'config'}->{'force_array'} = 0;
    foreach my $ignore_database (@$ignore_databases) {
        $self->{'ignore_databases'}{$ignore_database} = 1;
    }
    $self->{'host'} = $host;
    $self->{'port'} = $port;   

    my $user = $self->{'config'}->get( "/config/mongo/$privilege" );
    $self->{'user'}     = $user->{'user'};
    $self->{'password'} = $user->{'password'};

    log_debug( "Connecting to MongoDB as $privilege user on $host:$port." );

    eval {
        $self->{'mongo'}   = MongoDB::MongoClient->new( 
            host     => "$host:$port", 
            username => $user->{'user'},
            password => $self->{'password'}
        );
    };
    if($@){
        $self->error("Couldn't establish $privilege database connections: $@");
        return;
    }

    $self->json( JSON->new() );

    return $self;
}

sub get_database {
    my $self = shift;
    my $name = shift;
    my %args = @_;

    my $create = $args{'create'};
    my $drop   = $args{'drop'};

    my $mongo = $self->mongo();

    # if we're force creating this just do it, that's what the driver does
    if ($create){
        $self->_grant_db_role( db => $name ) || return;
	return $mongo->get_database($name);
    }
    if ($drop){
        $self->_revoke_db_role( db => $name ) || return;
        my $db = $mongo->get_database($name);
        if(!$db->drop()){
            $self->error( "Error deleting database: ".$db->last_error());
            return;
        }
        return 1;
    }
    
    my @existing_dbs = $self->_database_names();
    if (! grep { $_ eq $name } @existing_dbs){
	$self->error("Unknown database \"$name\"");
	return;
    }

    return $mongo->get_database($name);
}

# The perl driver as of version 1.8 does not support using nameOnly parameter 
# when fetching the database names, saving a number of locks and other such operations.
# We to date never care about sizes or other info that this command returns such MongoClient->database_names
# has only ever returned the names anyway
sub _database_names {
    my ($self) = @_;

    my $results = $self->mongo()->get_database('admin')->run_command(["listDatabases" => 1,
								      "nameOnly" => 1]);

    return if (! $results->{'ok'});
    return map { $_->{'name'} } @{$results->{'databases'}};
}

sub get_databases {
    my ($self, %args) = @_;
    
    my $mongo = $self->mongo();

    my @all_databases = $self->_database_names();

    return if ( !@all_databases);

    my @databases;
    foreach my $database (@all_databases) {
        # skip it if its a 'private' database prefixed with _ or one of the listed databases to ignore
        if ( $database =~ /^_/ || IGNORE_DATABASES->{$database} || $self->{'ignore_databases'}{$database} ) {
            log_debug( "Ignoring database $database." );
            next;
        }
        if (! $self->_has_access_to( $database ) ){
            log_debug( "Skipping unauthorized or non TSDS database $database." );
            next;
        }
        push(@databases, $database);
    }

    return \@databases;
}

sub _has_access_to {
    my ( $self, $db_name ) = @_;

    eval {
        my $res = $self->mongo()->get_database( $db_name )->get_collection( 'metadata' )->find_one();
    };
    # if there was an error
    if ($@){
        # if the error was just that we're not authorized, that's fine and skip this
        if ($@ =~ /not authorized/){
            return 0;
        }
        # otherwise propagate the error upwards
        die $@;
    }

    return 1;
}

sub _get_tsds_users {
    my ( $self ) = @_;

    my $ro_user = $self->{'config'}->get( "/config/mongo/readonly" );
    my $rw_user = $self->{'config'}->get( "/config/mongo/readwrite" );

    $ro_user->{'role'} = 'read';
    $rw_user->{'role'} = 'readWrite';


    return [$ro_user, $rw_user];
}

sub _grant_db_role {
    my ($self, %args) = @_;
    my $db = $args{'db'};
    


    foreach my $user (@{$self->_get_tsds_users()}) {
        my $username = $user->{'user'};
        my $password = $user->{'password'};
        my $role     = $user->{'role'}; 

        my $response_json = $self->_execute_mongo(
            "db.getSiblingDB(\"admin\").grantRolesToUser(\"$username\", [{role: \"$role\", db: \"$db\"}])"
        ) or return;
        if ( $response_json->{'ok'} ne '1') {
            $self->error( "Error while adding db $role role to user $user: " . $response_json->{'errmsg'} );
            return;
        }
    }

    return 1;
}

sub _revoke_db_role {
    my ($self, %args) = @_;
    my $db = $args{'db'};

    foreach my $user (@{$self->_get_tsds_users()}) {
        my $username = $user->{'user'};
        my $password = $user->{'password'};
        my $role     = $user->{'role'}; 

        my $response_json = $self->_execute_mongo(
            "db.getSiblingDB(\"admin\").revokesRolesFromUser(\"$username\", [{role: \"$role\", db: \"$db\"}])"
        ) or return;
        if ( $response_json->{'ok'} ne '1') {
            $self->error( "Error while adding db $role role to user $user: " . $response_json->{'errmsg'} );
            return;
        }
    }

    return 1;
}

# a nice wrapper to avoid auto vivifying databases or collections
sub get_collection {
    my $self      = shift;
    my $db_name   = shift;
    my $col_name  = shift;
    my %args      = @_;

    my $create = $args{'create'};
    
    # make sure db exists 
    my $db = $self->get_database($db_name);
    if(!$db){
        $self->error("Can't get collection $col_name in db $db_name, $db_name does not exists");
        return;
    }

    # we're usually using the same name for the database and the collection
    # so let's be nice and lazy about it
    if (! $col_name){
        $col_name = $db_name;
    }

    # if we want to auto vivify if necessary, go ahead and do it. it's the driver
    # default so we just chain the calls together
    if ($create){
        return $self->create_collection( $db_name, $col_name );
    }
    
    return $db->get_collection($col_name);    
}

# don't seem to be able to create a collection via autovivify, not sure if 3.0.0 thing or what
#
sub create_collection {
    my ( $self, $db_name, $col_name, %args) = @_;
    
    # make sure db_name was passed in
    if(!defined($db_name)){
        $self->error("Must specify database name when creating collection");
        return;
    }
    # make sure col_name was passed in
    if(!defined($col_name)){
        $self->error("Must specify column name when creating collection");
        return;
    }

    # make sure database exists
    my $db = $self->get_database($db_name);    
    eval {
	my $result = $db->run_command([create => $col_name]);
    }; 
    die $@ if ($@ && $@ !~ /already exists/);

    return $self->get_collection( $db_name, $col_name );
}

# needed to make our own rename_collection method here since
# the current mongodb driver fails if the collection already 
# exists without an option to pass in dropTarget
sub rename_collection {
    my ($self, $db_name, $col_name, $new_col_name) = @_;

    if(!$self->get_database($db_name)){
        $self->error("Database, $db_name, does not exist");
        return;
    }
    if(!$self->get_collection($db_name, $col_name)){
        $self->error("Collection, $col_name. does not exist");
        return;
    }

    my $admin = $self->get_database('admin');
    my $obj = $admin->run_command([
        renameCollection => "$db_name.$col_name",
        to => "$db_name.$new_col_name",
        dropTarget => 1
    ]);

    if(!ref($obj)){
        $self->error("Error renaming collection: ".$obj);
        return;
    }
    if(ref($obj) eq 'HASH' && !defined($obj->{'ok'})){
        $self->error("Error renaming collection: ".$self->_pp($obj));
        return;
    }

    return 1;
}

sub error {
    my $self = shift;
    my $err  = shift;

    if ($err){
	$self->{'error'} = $err;
	log_error($err);
    }

    return $self->{'error'};
}

sub json {
    my $self = shift;
    my $json  = shift;

    if ($json){
        $self->{'json'} = $json;
    }

    return $self->{'json'};
}

sub add_shard {
    my ( $self, %args) = @_;

    #add a mongod shard file
    #my $response_json = $self->_execute_mongo( "sh.addShard( \"localhost:27025\" )" ) or return;
    my $hostname = hostname();
    my $response_json = $self->_execute_mongo( "sh.addShard( \"$hostname:27025\" )" ) or return;

    return 1;
}

sub enable_sharding {

    my ( $self, $db_name ) = @_;


    my $db = $self->get_database("admin");

    my $error;
    eval {
	$db->run_command([enableSharding => $db_name]);
    };
    $error = $@->message if ($@);

    return 1 if (! $error || $error =~ /sharding already enabled/);

    $self->error( "Unable to enable sharding on $db_name: $error" );
    return;
}

sub add_collection_shard {
    my ( $self, $db_name, $col_name, $shard_key ) = @_;

    my $response_json = $self->_execute_mongo( 
        "sh.shardCollection(\"$db_name\.$col_name\", $shard_key)"
    ) or return;

    if ( $response_json->{'ok'} ne '1') {
        # ignore error if its due to it already being sharded
        if ( $response_json->{'errmsg'} !~ /already enabled/ ) {
            $self->error( "Error while sharding $db_name $col_name: " . $response_json->{'errmsg'} );
            return;
        }
    }

    return 1;
}

sub _execute_mongo {

    my ( $self, $command, %args ) = @_;

    $command =~ s/'/\'/g;

    # default to root in the execute mongo case since
    my $database = defined($args{'database'}) ? $args{'database'} : "admin";

    my $line = "mongo".
               "  --host $self->{'host'}".
               "  --port $self->{'port'}".
               "  --username '$self->{'user'}'".
               "  --password '$self->{'password'}'".
               "  $database".
               "  --eval 'JSON.stringify($command);'";

    my $output = `$line`;

    #warn "output: $output";
    #print "line: '$line'\n\n";
    # Output will look like:
    # MongoDB shell version: 2.6.5
    # connecting to: test
    # $json_response

    # if thing already exists consider it a success
    if($output =~ /already exists/){
        return { ok => 1, output => $output };
    }


#    for ( my $i = 0; $i < @lines; $i++ ) {
#
#        my $line = $lines[$i];
#
#        next if ( $line !~ /^connecting to: / );
#
#        $response_json = $lines[$i + 1];
#        #last;
#    }
#
    # exclude all the cruft that comes before the json
    my @lines = split(/\n/, $output);
    my @json_lines;
    my $start_parsing = 0;
    foreach my $line (@lines) {
        if( $line =~ /^{/){
            $start_parsing = 1;
        }
        next if(!$start_parsing);
        push(@json_lines, $line);
    }


    #warn "lines: ".Dumper(\@json_lines);
    # if the response was only one line, just pick out the json
    my $response_json;
    # when granting and revoking roles to a user, no output is returned
    # treat 0 json_lines as a success
    if(@json_lines == 0){
        return { ok => 1, output => $output };
    }
    elsif(@json_lines == 1){
        ( $response_json ) = $json_lines[0] =~ /.*?({.*}).*/;
    } else {
        foreach my $line (@json_lines) {
            # don't start parsing until we hit opening brace
            $response_json .= $line;
        }

        if ( !$response_json ) {
            $self->error("Error running mongo command $command: $output");
            return;
        }
    }

    my $json;
    eval {
        $response_json = $self->json()->decode( $response_json );
    };
    if ($@){
        $self->error("Error parsing mongo response: $output: $@");
        return;
    }

    # if response didn't include ok just add it for consistencies sake
    $response_json->{'ok'} = 1 if(!defined($response_json->{'ok'}));

    #warn "response_json_parsed: ".Dumper($response_json);
    return $response_json;
}

# recursively loops through metadata adding indexes for each field
sub process_meta_index {

    my ( $self, %args ) = @_;

    my $database = $args{'database'};
    my $prefix   = $args{'prefix'};
    my $data     = $args{'data'};

    # get a flat list of all the meta_data fields
    my $fields = $self->flatten_meta_fields(
        data   => $data,
        prefix => $prefix
    );
    # handle every field at this level of the metadata
    foreach my $field ( keys %$fields ) {
        log_info("Adding index for $field");
        my $indexes = $database->get_collection( 'measurements' )->indexes();
	$indexes->create_one([$field => 1]);
    }

    # This is suboptimal but ensures tidiness - after adding
    # a metadata field, re-check its parent to ensure that it isn't
    # indexed. Parent fields are never referenced in TSDS, they
    # exist only as containers for subtypes. Since there is a limit
    # of 64 indexes in Mongo, we need to be prudent about what we index
    my $metadata = $database->get_collection('metadata')->find_one();

    foreach my $base_field (keys %{$metadata->{'meta_fields'}}){
	if (keys %{$metadata->{'meta_fields'}{$base_field}{'fields'}}){
	    my $index = $database->get_collection('measurements')->indexes();
	    foreach my $existing ($index->list()->all()){
		if (exists $existing->{'key'}{$base_field} && keys %{$existing->{'key'}} == 1){
		    log_info("Dropping index for parent field $base_field");
		    $index->drop_one($existing->{'name'});
		}
	    }
	}
    }
}

# returns a flat list of all meta fields and their children
sub flatten_meta_fields {
    my ( $self, %args ) = @_;

    #my $database = $args{'database'};
    my $meta_fields = {};
    my $prefix      = $args{'prefix'};
    my $data        = $args{'data'};

    foreach my $field ( keys %$data ) {
        my $attrs = dclone( $data->{$field} );
        delete $attrs->{'fields'};
        my $total_field = $prefix . $field;       
        my $sub_meta_fields = {};
        if ( exists $data->{$field}{'fields'} ) {
            $sub_meta_fields = $self->flatten_meta_fields( 
                prefix => "$total_field\.",
                data => $data->{$field}{'fields'},
            );
        }
	else {
	    $meta_fields->{$total_field} = $attrs;
	}
	
	%$meta_fields = ( %$meta_fields, %$sub_meta_fields );	
    }

    return $meta_fields;
}

sub mongo {
    my ($self, %args) = @_;

    return $self->{"mongo"};
}

sub _pp {
   my $obj = shift;

   my $dd = Data::Dumper->new([$obj]);
   $dd->Terse(1)->Indent(0);

   return $dd->Dump();
}

1;
