#--------------------------------------------------------------------
#----- GRNOC TSDS MetaData DataService Library
#-----
#----- Copyright(C) 2015 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/DataService/MetaData.pm $
#----- $Id: MetaData.pm 39750 2015-10-16 13:23:46Z daldoyle $
#-----
#----- This module inherits the base GRNOC::TSDS::DataService class
#----- and provides all of the methods to interact with metadata
#--------------------------------------------------------------------

package GRNOC::TSDS::DataService::MetaData;

use strict;
use warnings;

use base 'GRNOC::TSDS::DataService';

use GRNOC::Log;
use GRNOC::TSDS::MongoDB;
use GRNOC::TSDS::Parser;
use GRNOC::TSDS::Constraints;

use Storable qw(dclone);
use DateTime;
use DateTime::Format::Strptime;
use Data::Dumper;
use JSON;
use Data::Compare;

### constants ###
use constant DEFAULT_COLLECTIONS => ['data', 'event', 'measurements', 'metadata', 'aggregate', 'expire'];

# this will hold the only actual reference to this object
my $singleton;

sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    # if we've created this object (singleton) before, just return it
    return $singleton if ( defined( $singleton ) );

    my $self = $class->SUPER::new( @_ );

    bless( $self, $class );

    # store our newly created object as the singleton
    $singleton = $self;

    # get/store all of the data services we need
    $self->mongo_rw( GRNOC::TSDS::MongoDB->new( @_, privilege => 'rw' ) );
    $self->mongo_ro( GRNOC::TSDS::MongoDB->new( @_, privilege => 'ro' ) );
    $self->mongo_root( GRNOC::TSDS::MongoDB->new( @_, privilege => 'root' ) );

    $self->parser( GRNOC::TSDS::Parser->new( @_ ) );

    return $self;
}

# GET METHODS
sub get_measurement_types {
    my ($self, %args) = @_;

    my $results = $self->mongo_ro()->get_databases();
    my $show_classifiers = defined($args{'show_classifiers'}) ? $args{'show_classifiers'} : 1;

    if ( !$results ) {
        $self->error( 'Error getting Databases.' );
        return;
    }

    my $databases = $self->_get_constraint_databases();

    my @measurement_types;

    foreach my $type (@$results) {

        next if ($type =~ /^_/);
        next if ($type eq 'admin' ||
                 $type eq 'config' ||
                 $type eq 'test' ||
                 $type eq 'tsds_reports');

        next if ( defined($databases) and (! (grep $_ eq $type, @$databases)) );

        my $meta_collection = $self->mongo_ro()->get_collection($type, 'metadata');
        next if(!$meta_collection);

        # get measurements collection if count flag was passed in
        my $meas_collection;
        if($args{'show_measurement_count'}){
            $meas_collection = $self->mongo_ro()->get_collection($type, 'measurements');
            next if(!$meas_collection);
        }

        my $fields         = $meta_collection->find_one();
        my $label          = $fields->{'label'};
        my $data_doc_limit = $fields->{'data_doc_limit'};
        my $event_limit    = $fields->{'event_limit'};
        my $search_weight  = $fields->{'search_weight'};
        my $ignore_si      = $fields->{'ignore_si'};

        my $measurement_type = {
            'name' => $type,
            'label'=> $label,
            'event_limit' => $event_limit,
            'data_doc_limit' => $data_doc_limit,
            'search_weight' => $search_weight,
            'ignore_si' => $ignore_si
        };
        # add count if flag was passed in
        if($args{'show_measurement_count'}){
            my $mt_count = $meas_collection->count();
            $measurement_type->{'measurement_count'} = $mt_count;
        }
        # add an array of the required fields if the flag was passed in
        if($args{'show_required_fields'}){
            my $required_fields = [];
            foreach my $field (keys %{$fields->{'meta_fields'}}){
                push(@$required_fields, $field) if($fields->{'meta_fields'}{$field}{'required'});
            }
            $measurement_type->{'required_fields'} = $required_fields;
        }
        push (@measurement_types, $measurement_type);

        # now add classifier fields if we have any
        if($show_classifiers){
            my $classifiers = $self->_get_classifier_fields($fields->{'meta_fields'});
            next if(!$classifiers);
            foreach my $classifier (@$classifiers) {
                my $measurement_type = {
                    'name'    => $classifier->{'name'},
                    'parent'  => $type,
                    'label'   => $label,
                };
                # get count if flag was passed in
                if($args{'show_measurement_count'}){
                    my $empty = ($classifier->{'array'}) ? [] : undef;
                    my $c_count = $meas_collection->count( { $classifier->{'name'} => { '$ne' => $empty } });
                    $measurement_type->{'measurement_count'} = $c_count
                }
                # add an array of the required fields if the flag was passed in
                if($args{'show_required_fields'}){
                    my $required_fields = [];
                    foreach my $field (keys %{$classifier->{'fields'}}){
                        push(@$required_fields, $field) if($classifier->{'fields'}{$field}{'required'});
                    }
                    $measurement_type->{'required_fields'} = $required_fields;
                }
                push (@measurement_types, $measurement_type);
            }
        }
    }

    return \@measurement_types;
}

sub get_meta_fields {
    my ( $self, %args ) = @_;
    
    my ($classifier, $measurement_type) = $self->_parse_measurement_type( $args{'measurement_type'} );

    # keeping this for backwards compatibility in the frontend, this way not ideal
    # should pass in the measurment_type as $measurement_type.$classifier in the classifier case
    $classifier = $args{'meta_field'} if(defined($args{'meta_field'}));

    # fetch out meta_fields from mongo
    my $meta_collection = $self->mongo_ro()->get_collection($measurement_type, "metadata");
    if (! $meta_collection ) {
        $self->error( 'Invalid Measurement Type.' );
        return;
    }
    my $fields = $meta_collection->find_one();
    if (! $fields ) {
        $self->error( 'Invalid Measurement Type.' );
        return;
    }

    my $meta = $fields->{'meta_fields'};
    my $meta_fields = $self->_get_meta_field_helper($meta);

    # sort by ordinal field here, we need it sorted this way almost everywhere so ✔ #justdoit
    $meta_fields = $self->_sort_meta_fields_by_ordinal( $meta_fields );

    # apply some filters if passed in 
    $meta_fields = $self->_filter_fields( $meta_fields,
        is_ordinal    => $args{'is_ordinal'},
        is_required   => $args{'is_required'},
        is_classifier => $args{'is_classifier'}
    );

    return $meta_fields;
}

# creates a hash that's easy for interfacing applications to look up meta information about a given measurement_type
sub get_measurement_type_schemas {
    my ( $self, %args ) = @_;

    # validate measurement_types passed in exists 
    my $measurement_types = [];
    if( $args{'measurement_type'} ) {
        foreach my $type (@{$args{'measurement_type'}}) {
            if (!$self->mongo_ro()->get_database($type) ) {
                $self->error('Invalid measurement type: ' . $type);
                return;
            }
            push(@$measurement_types, $type);
        }
    } 
    # if no measurement_types where passed in grab them all
    else {
        my $all_measurement_types = $self->get_measurement_types( show_classifiers => 0 );
        foreach my $type (@$all_measurement_types) {
            push @$measurement_types, $type->{'name'};
        }
    }

    # now create an array with all the 
    my $schemas = {};
    foreach my $measurement_type (@$measurement_types){
        # grab the value and meta_fields for this measurement_type
        my $value_fields = $self->get_measurement_type_values( measurement_type => $measurement_type );
        my $meta_fields  = $self->get_meta_fields( measurement_type => $measurement_type ); 


        # filter out various types of meta_fields for easy lookup
        my $classifier_meta_fields = $self->_filter_fields( $meta_fields,
            is_classifier => 1
        );
        @$classifier_meta_fields = sort _by_ordinal( @$classifier_meta_fields );
        my $ordinal_meta_fields = $self->_filter_fields( $meta_fields,
            is_ordinal => 1
        );
        @$ordinal_meta_fields = sort _by_ordinal( @$ordinal_meta_fields );
        my $required_meta_fields = $self->_filter_fields( $meta_fields,
            is_required => 1
        );
        @$required_meta_fields = sort _by_ordinal( @$required_meta_fields );

        # filter out various types of value_fields for easy lookup
        my $ordinal_value_fields = $self->_filter_fields( $value_fields,
            is_ordinal => 1 
        );
        @$ordinal_value_fields = sort _by_ordinal( @$ordinal_value_fields );

        # create lookup hashes of the meta and value fields
        my $meta_field_lookup  = $self->_create_field_lookup_hash( $meta_fields,
            flatten_fields => $args{'flatten_fields'}
        );
        my $value_field_lookup = $self->_create_field_lookup_hash( $value_fields,
            flatten_fields => $args{'flatten_fields'}
        );
        
        # grab the label for the measurement_type 
        my $metadata = $self->mongo_ro()->get_collection($measurement_type, 'metadata');
        if(!$metadata){
            $self->error("Invalid measurement_type, $measurement_type, skipping...");
            next;
        }
        my $label = $metadata->find_one()->{'label'};
        my $ignore_si = $metadata->find_one()->{'ignore_si'};

        $schemas->{$measurement_type} = {
            label => $label,
            ignore_si => $ignore_si,
            meta => {
                fields     => $meta_field_lookup,
                required   => [map { $_->{'name'} } @$required_meta_fields],
                ordinal    => [map { $_->{'name'} } @$ordinal_meta_fields],
                classifier => $self->_create_classifier_lookup_hash( $classifier_meta_fields )
            },
            value => {
                fields  => $value_field_lookup,
                ordinal => [map { $_->{'name'} } @$ordinal_value_fields]
            }
        };
    }

    return $schemas;
}

# creates a lookup hash for classifiers where the key is the field that is classified
# and the valeus are the fields that uniquely identify the classfier, should be ordinal keys
# in the case that the classifier has subfileds or just the key of the field if its a simple key value pair 
# (i.e.) the lookup for the key value pair case will have the same value for both the key and the value
# {
#   node: ['node']                            # example of simple key value pair classifier
#   circuit: ['circuit.name', 'circuit.desc'] # example of tree structured classifier
# }
sub _create_classifier_lookup_hash {
    my ($self, $fields) = @_;

    # loop through the classifier fields creating lookups for them
    my $classifier_lookup = {};
    foreach my $field (@$fields){
        my $name       = $field->{'name'};
        my $sub_fields = $field->{'fields'};

        my $classifier_strings = [];
        if($sub_fields){ 
            my $ordinal_sub_fields = $self->_filter_fields( $sub_fields,
                is_ordinal => 1
            );

            foreach my $ordinal_sub_field (@$ordinal_sub_fields){
                push(@$classifier_strings, $name.'.'.$ordinal_sub_field->{'name'} );
            }
        } else {
            $classifier_strings = [$name];
        }
        $classifier_lookup->{$name} = { ordinal => $classifier_strings };
    }

    return $classifier_lookup;
}

# creates a lookup hash of any meta_fields or value_fields, containing subfields as values, as well
# as display hints such as label, units, etc.
sub _create_field_lookup_hash {
    my ($self, $fields, %args) = @_;
    my $flatten_fields = $args{'flatten_fields'} || 0;

    my $field_lookup = {};
    foreach my $f (@$fields) {
        my $name       = $f->{'name'};
        my $sub_fields = $f->{'fields'};

        my $field = dclone($f);
        # remove oridinal, required, name, fields, and classifier keys as there values
        # will be apparent in the structure returned by the get_measurement_type_schemas method

        delete $field->{'name'};
        delete $field->{'fields'};
        delete $field->{'array'};
        defined($field->{'ordinal'})  ? ($field->{'ordinal'} += 0)  : delete $field->{'ordinal'};
        defined($field->{'required'}) ? ($field->{'required'} += 0) : delete $field->{'required'};
        delete $field->{'classifier'};

        # if there's any subfields recursively format them and merge them back into our lookup
        if(defined($sub_fields)){
            if($flatten_fields) {
                my $hash = $self->_create_field_lookup_hash( $sub_fields, %args );
                foreach my $key (keys %$hash){
                    my $value = $hash->{$key};
                    $field_lookup->{$name.'.'.$key} = $value;
                }
            } else {
                $field_lookup->{$name}{'fields'} = $self->_create_field_lookup_hash( $sub_fields, %args );
            }
        } else {
            $field_lookup->{$name} = { %$field };
        }
    }

    return $field_lookup;
}

#TODO should replace get_measurement_type_values method with get_value_fields for more consistent method names
sub get_measurement_type_values {
    my ( $self, %args ) = @_;
    my $measurement_type = $args{'measurement_type'};
    my $classifier;
    ($classifier, $measurement_type) = $self->_parse_measurement_type( $measurement_type );
    
    # grab the metadata collection and make sure it exists
    my $meta_collection = $self->mongo_ro()->get_collection($measurement_type, "metadata");
    if (! $meta_collection ) {
        $self->error( "Invalid measurement_type: $measurement_type" );
        return;
    }

    # make sure the meta collection has fields and grab the values 
    my $fields = $meta_collection->find_one();
    if (! $fields ) {
        $self->error( "Invalid measurement_type: $measurement_type" );
        return;
    }
    my $values = $fields->{'values'};

    # create a list of our value fields
    my $value_fields = [];
    foreach my $name (keys %$values) {
        my $value_field = {
            name => $name,
            %{$values->{$name}}
        };
        push(@$value_fields, $value_field);       
    }       

    # apply some filters if passed in 
    $value_fields = $self->_filter_fields( $value_fields,
        is_ordinal  => $args{'is_ordinal'}
    );

    return $value_fields;
}

#TODO should determine dependendencies for this method replace them with a call get get_measurement_type_values/get_value_fields with filters
sub get_measurement_type_ordinal_values {
    my ( $self, %args ) = @_;

    my $measurement_type = $args{'measurement_type'};
    my @meta_fields = @{ $self->get_measurement_type_values( 
        %args 
    ) };

    my @meta_fields_sorted = sort _by_ordinal( @meta_fields );
    my @ordinal_fields = ();
    foreach my $field (@meta_fields_sorted) {        
        push @ordinal_fields, $field if ( exists $field->{'ordinal'} );

    }

    return \@ordinal_fields;
}

sub get_meta_field_values {
    my ( $self, %args ) = @_;

    my $measurement_type = $args{'measurement_type'};
    my $meta_field       = $args{'meta_field'};
    my $limit            = $args{'limit'};
    my $offset           = $args{'offset'};

    my $classifier;
    ($classifier, $measurement_type) = $self->_parse_measurement_type( $measurement_type );

    return if(!$measurement_type || !$meta_field);

    my $date_format = DateTime::Format::Strptime->new( pattern => '%m/%d/%Y %H:%M:%S');

    my $base_time    = $date_format->format_datetime($date_format->parse_datetime('01/01/1970 00:00:00'));
    my $present_time = $date_format->format_datetime(DateTime->now());

    my $query = "get $meta_field between(\"$base_time\", \"$present_time\") by $meta_field from $measurement_type";

    my %logical_operator = map { $_ => $args{$_} } grep { $_ =~ /_logic$/ } keys %args;
    my %meta_filters = map { $_ => $args{$_} } grep { 
        $_ ne 'measurement_type' && 
        $_ ne 'meta_field' &&
        $_ ne 'limit' &&
        $_ ne 'offset' &&
        $_ ne 'order' &&
        $_ !~ /_logic$/ 
    } keys %args;
    $query .= " where" if (%meta_filters);

    my %operators = (
        "_"        => "=" ,
        "not"      => "!=",
        "like"     => "like",
        "not_like" => "not like"
    );

    my %filters;
    foreach my $key (keys %meta_filters) {
        my $field = $key;
        my $logic;

        # if the field has logic in its name, detect that
        # something like intf_like or intf_not_like
        foreach my $op (keys %operators){
            $logic = $op if ($key =~ /_$op$/);
        }

        if ($logic){
            $key =~ /^(.+)_$logic$/;
            $field = $1;
        }

        $logic = "_" if !$logic;
        $filters{$field}{$logic} = $meta_filters{$key};
    }

    foreach (keys %filters ) {
        $query .= " (";
        my $meta_field = $_;
        my $logic = lc($logical_operator{$meta_field."_logic"});

        foreach ( keys  %{$filters{$meta_field}} ) {
            my @meta_values = @{$filters{$meta_field}{$_}};
            my $operator = $operators{$_};   

            foreach (@meta_values) {
                $query .= " $meta_field $operator \"$_\"";
                $query .= " $logic" if $_ ne (@meta_values)[-1];
            }
            $query .= " $logic" if $_ ne (keys %{$filters{$meta_field}})[-1];
        }

        $query .= " )";
        $query .= " and" if $_ ne (keys %filters)[-1];
    }

    $query .= " limit $limit offset $offset";
    $query .= " ordered by $meta_field";

    log_debug("Query passed to query engine: $query");

    my $results = $self->parser()->evaluate($query);

    if ( !$results) {
        $self->error( 'Error getting meta fields values.' );
        return;
    }
    my @meta_field_values;
    foreach (@{$results}) {
        push (@meta_field_values , {   
            value => $_->{$meta_field}
        });
    }

    return \@meta_field_values;
}

sub _get_meta_field_helper {
    my ( $self, $meta, $required, $parent, ) = @_;

    my @meta_fields;
    foreach(keys %{$meta}) {
        my $meta_required = defined($meta->{$_}->{'required'}) ?  $meta->{$_}->{'required'} : $required ;
        my $name = $_;

        my %field = ( name => $name, required  => $meta_required );
        $field{'ordinal'}    = $meta->{$_}->{'ordinal'} if $meta->{$_}->{'ordinal'};
        $field{'array'}      = $meta->{$_}->{'array'} if $meta->{$_}->{'array'};
        $field{'classifier'} = $meta->{$_}->{'classifier'} if $meta->{$_}->{'classifier'};
        $field{'search_weight'} = $meta->{$_}->{'search_weight'} if exists $meta->{$_}->{'search_weight'};

        if ($meta->{$_}->{'fields'}) {
            $field{'fields'} = $self->_get_meta_field_helper(
                $meta->{$_}->{'fields'},
                $meta_required,
                $name 
            );
            push (@meta_fields,\%field);
        } else {
            push (@meta_fields,\%field);
        }
    }
    return \@meta_fields;
}

sub _get_classifier_fields {
    my ( $self, $meta_fields ) = @_;

    my @classifier_fields;
    foreach my $name (keys %$meta_fields) {
        my $classifier = defined($meta_fields->{$name}{'classifier'}) ? $meta_fields->{$name}{'classifier'} : 0 ;
        next if(!$classifier);
        $meta_fields->{$name}{'name'} = $name;
        push(@classifier_fields, $meta_fields->{$name});
    }

    return \@classifier_fields;
}

# ADD METHODS
sub add_measurement_type {

    my ( $self, %args ) = @_;

    my $measurement_type  = $args{'name'};
    my $label = $args{'label'};
    my $ignore_si = $args{'ignore_si'} || 0;
    my $required_meta_fields = $args{'required_meta_field'};
    my $search_weight;
    $search_weight = $self->parse_int($args{'search_weight'}) if(exists($args{'search_weight'}));

    # sanity checks
    if(!defined($measurement_type)){
        $self->error( 'Must pass in a name for the measurement type.' );
        return;
    }
    if(!defined($label)){
        $self->error( 'Must pass in a label for the measurement type.' );
        return;
    }
    if(!defined($required_meta_fields)){
        $self->error( 'Must pass in a required_meta_fields for the measurement type.' );
        return;
    }
    # search_weight is defined as $NUMBER_ID, which allows 0
    # we don't want to allow a 0 zero value; enforce here
    if (defined $search_weight && $search_weight == 0) {
        $self->error("Search weight must be a positive integer, or blank.");
        return;
    }

    # format the meta data fields with just the required field and name set
    my $meta_fields = {};
    my $ordinal = 1;
    foreach my $meta_field (@$required_meta_fields){
        $meta_fields->{$meta_field} = {
            'required' => 1,
            'ordinal' => $ordinal++
        };
    }

    # add the measurement_type
    my $db = $self->mongo_root()->get_database( $measurement_type, create => 1, privilege => 'root' );

    # enable sharding on the measurement_type
    if(!$self->mongo_root()->enable_sharding( $measurement_type )){
        $self->error( 'Error enabling sharding on measurement_type: '.$self->mongo_root()->error() );
        return;
    }

    # create the default collections for a measurement_type and ensure indexes on each of them
    foreach my $col_name (@{DEFAULT_COLLECTIONS()}){
        if( $col_name eq 'data' || $col_name eq 'measurements' || $col_name eq 'event' ){

            # events and data/measurements are sharded differently
            my $shard_key = $GRNOC::TSDS::MongoDB::DATA_SHARDING;
            if ($col_name eq 'event'){
                $shard_key = $GRNOC::TSDS::MongoDB::EVENT_SHARDING;
            }

            # add shard for collection, this also creates the collection it seems
            if(!$self->mongo_root()->add_collection_shard( $measurement_type, $col_name, $shard_key )){
                $self->error( "Error adding collection shard for $col_name measurement_type: ".$self->mongo_root()->error() );
                return;
            }
        } 
	    else {
            # otherwise explicitly create the collection
            $self->mongo_root()->create_collection( $measurement_type, $col_name, privilege => 'root' );
        }
        my $collection = $db->get_collection( $col_name );
        $collection->ensure_index({start => 1});
        $collection->ensure_index({end   => 1});

        if( $col_name eq 'data' ){
            my $index = Tie::IxHash->new(
                identifier => 1,
                start      => 1,
                end        => 1
            );
            $collection->ensure_index($index);
	    $collection->ensure_index({updated => 1});
        }

	if ( $col_name eq 'measurements' ){
	    $collection->ensure_index({identifier => 1});
	    $collection->ensure_index({last_updated => 1});
	}
    }

    # insert meta data into the metadata collections
    $db->get_collection( 'metadata' )->insert({ 
        label         => $label,
        ignore_si     => $ignore_si,
        meta_fields   => $meta_fields,
        search_weight => $search_weight
    });
    $db->get_collection( 'metadata' )->update({}, { '$set' => { values => {} } } );

    # now create indexes for all of the combinations of meta data
    $meta_fields = $db->get_collection( 'metadata' )->find_one( {} )->{'meta_fields'};

    $self->mongo_root()->process_meta_index(
        database => $db,
        prefix => "",
        data => $meta_fields
    );
   
    # insert the default expire record
    # get the aggregate collections
    my $exp_col = $self->mongo_root()->get_collection($measurement_type, "expire", privilege => 'root' );
    if(!$exp_col){
        $self->error($self->mongo_root()->error());
        return;
    }
    my $id = $exp_col->insert({
        name          => 'default',
        max_age       => 630720000,
        meta          => "{}",
        eval_position => 10
    });
    if(!$id) {
        $self->error( "Error creating default aggregate");
        return;
    }

    return [{ 'success' => 1 }];
}

#TODO change method to add_value_field
sub add_measurement_type_value {
    my ( $self, %args ) = @_;
    my $measurement_type = $args{'measurement_type'};
    my $name             = $args{'name'};
    my $descr            = $args{'description'};
    my $units            = $args{'units'};
    my $ordinal          = $args{'ordinal'};

    my $db  = $self->mongo_rw()->get_database( $measurement_type );
    if(!$db){
        $self->error($self->mongo_rw()->error());
        return;
    }
    my $col = $db->get_collection( "metadata" );
    if(!$col){
        $self->error($self->mongo_rw()->error());
        return;
    }

    my $value = {
        description => $descr,
        units => $units
    };
    $value->{'ordinal'} = int($ordinal) if(defined($ordinal));

    my $id = $col->update({}, { '$set' => { "values.$name" => $value } } );
    if(!$id) {
        $self->error( "Error adding new value, $name to measurement type, $measurement_type");
        return;
    }

    return [{ 'success' => 1 }];
}

sub add_meta_field {
    my ( $self, %args ) = @_;

    my $measurement_type = $args{'measurement_type'};
    my $name             = $args{'name'};
    my $classifier       = $args{'classifier'};
    my $ordinal          = $args{'ordinal'};
    my $array            = $args{'array'};
    my $search_weight    = $args{'search_weight'};

    # search_weight is defined as $NUMBER_ID, which allows 0
    # we don't want to allow a 0 zero value; enforce here
    if (defined $search_weight && $search_weight == 0) {
        $self->error("Search weight must be a positive integer, or blank.");
        return;
    }

    my $db  = $self->mongo_rw()->get_database( $measurement_type );
    if(!$db){
        $self->error($self->mongo_rw()->error());
        return;
    }
    my $col = $db->get_collection( "metadata" );
    if(!$col){
        $self->error($self->mongo_rw()->error());
        return;
    }


    # verify any parent meta fields already exists and format name
    $name = $self->_format_meta_field_name( $name, collection => $col, exists => 1, add => 1 ) || return;
    
    my $meta_field = {};
    $meta_field->{'classifier'}    = int($classifier)    if(defined($classifier));
    $meta_field->{'ordinal'}       = int($ordinal)       if(defined($ordinal));
    $meta_field->{'array'}         = int($array)         if(defined($array));
    $meta_field->{'search_weight'} = int($search_weight) if(defined($search_weight));

    my $id = $col->update({}, { '$set' => { "meta_fields.$name" => $meta_field } } );
    if(!$id) {
        $self->error( "Error adding new meta field, $name to measurement type, $measurement_type");
        return;
    }

    # ensure indexes on the things
    my $meta_fields = $col->find_one()->{'meta_fields'};
    $self->mongo_rw()->process_meta_index(
        database => $db,
        prefix => "",
        data => $meta_fields
    );

    return [{ 'success' => 1 }];
}

# UPDATE METHODS
sub update_measurement_types {
    my ( $self, %args ) = @_;

    my $measurement_type = $args{'name'};
    if(!$measurement_type){
        $self->error( "Must pass in measurement_type" );
        return;
    }
    my $search_weight = $args{'search_weight'};
    # search_weight is defined as $NUMBER_ID, which allows 0
    # we don't want to allow a 0 zero value; enforce here
    if (defined $search_weight && $search_weight == 0) {
        $self->error("Search weight must be a positive integer, or blank.");
        return;
    }

    my $col = $self->mongo_rw()->get_collection($measurement_type, "metadata");
    if(!$col){
        $self->error($self->mongo_rw()->error());
        return;
    }

    my $set = {};
    $set->{'label'} = $args{'label'} if(exists($args{'label'}));
    $set->{'ignore_si'} = $args{'ignore_si'} if(exists($args{'ignore_si'}));
    $set->{'data_doc_limit'} = $self->parse_int($args{'data_doc_limit'}) if(exists($args{'data_doc_limit'}));
    $set->{'event_limit'}    = $self->parse_int($args{'event_limit'})    if(exists($args{'event_limit'}));
    $set->{'search_weight'}  = $self->parse_int($args{'search_weight'})  if(exists($args{'search_weight'}));

    if(%$set){
        my $id = $col->update({}, {'$set' => $set} );
        if(!$id) {
            $self->error( "Error updating measurement type, $measurement_type, label" );
            return;
        }
    }

    return [{ 'success' => 1 }];
}

# TODO change method name to update_value_fields
sub update_measurement_type_values {
    my ( $self, %args ) = @_;
    
    my $measurement_type = $args{'measurement_type'};
    my $name             = $args{'name'};
    my $descr            = $args{'description'};
    my $units            = $args{'units'};
    my $ordinal          = $args{'ordinal'};
    
    if(!$name){
        $self->error( "Must pass in the name of the value to update" );
        return;
    }

    my $db  = $self->mongo_rw()->get_database( $measurement_type );
    if(!$db){
        $self->error($self->mongo_rw()->error());
        return;
    }
    my $col = $db->get_collection( "metadata" );
    if(!$col){
        $self->error($self->mongo_rw()->error());
        return;
    }

    my $set = {};
    $set->{"values.$name.description"} = $descr             if(defined($descr));
    $set->{"values.$name.units"}       = $units             if(defined($units));
    $set->{"values.$name.ordinal"}     = defined($ordinal)  ? int($ordinal)       : undef; 

    if(!$set){
        $self->error( "You must pass in at least 1 field to update" );
        return;
    }

    my $id = $col->update({}, { '$set' => $set } );
    if(!$id) {
        $self->error( "Error updating value, $name in measurement type, $measurement_type");
        return;
    }

    return [{ 'success' => 1 }];
}

sub update_meta_fields {
    my ( $self, %args ) = @_;

    my $measurement_type = $args{'measurement_type'};
    my $name             = $args{'name'};
    my $classifier       = $args{'classifier'};
    my $ordinal          = $args{'ordinal'};
    my $array            = $args{'array'};
    my $search_weight    = $args{'search_weight'};

    if(!$name){
        $self->error( "Must pass in the name of the meta field to update" );
        return;
    }

    # search_weight is defined as $NUMBER_ID, which allows 0
    # we don't want to allow a 0 zero value; enforce here
    if (defined $search_weight && $search_weight == 0) {
        $self->error("Search weight must be a positive integer, or blank.");
        return;
    }

    my $db  = $self->mongo_rw()->get_database( $measurement_type );
    if(!$db){
        $self->error($self->mongo_rw()->error());
        return;
    }
    my $col = $db->get_collection( "metadata" );
    if(!$col){
        $self->error($self->mongo_rw()->error());
        return;
    }

    # verify the meta_fields already exists and format name
    # you should be able to edit anything on a required field other than the array flag
    my $not_required = defined($array) ? 1 : 0; 
    $name = $self->_format_meta_field_name( $name, 
        collection => $col, 
        exists => 1, 
        not_required => $not_required 
    ) || return;

    my $set = {};
    $set->{"meta_fields.$name.classifier"}    = defined($classifier)    ? int($classifier)    : undef; 
    $set->{"meta_fields.$name.ordinal"}       = defined($ordinal)       ? int($ordinal)       : undef; 
    $set->{"meta_fields.$name.array"}         = defined($array)         ? int($array)         : undef; 
    $set->{"meta_fields.$name.search_weight"} = defined($search_weight) ? int($search_weight) : undef;

    if(!$set){
        $self->error( "You must pass in at least 1 field to update" );
        return;
    }

    my $id = $col->update({}, { '$set' => $set } );
    if(!$id) {
        $self->error( "Error updating value, $name in measurement type, $measurement_type");
        return;
    }
   
    # ensure indexes on the things 
    my $meta_fields = $col->find_one()->{'meta_fields'};
    $self->mongo_rw()->process_meta_index(
        database => $db,
        prefix => "",
        data => $meta_fields
    );

    return [{ 'success' => 1 }];
}

# Update one or more measurements' metadata. This works historically as
# well. Data supplied is merged with existing metadata for the same
# time period, so updating just a single field is possible. Exception to
# that is arrayed fields - these are taken always as an entire rewrite
# since it's not possible to see which parts "changed" on a subfield.
sub update_measurement_metadata {
    my ( $self, %args ) = @_;

    my $updates = dclone($args{'values'});

    # lots of sanity checking first. This is a little inefficient
    # because we want to make sure we can sanity check everything
    # first to avoid processing N of M messages and realizing there's
    # a bad message. Mongo has no ACID but this gets a bit closer.
    my $metadata_cache = $self->_update_measurement_metadata_sanity_check($updates);
    return if (! defined $metadata_cache);

    # Okay at this point we know the messages look syntactically right according to
    # this method and the respective metadata docs, let's go ahead and start processing
    # the actual contents
    my $modified = 0;

    foreach my $obj (@$updates){
        my $start = delete $obj->{'start'};
        my $end   = delete $obj->{'end'};
        my $type  = delete $obj->{'type'};

        my $metadata = $metadata_cache->{$type};

        # Pull out the required fields so we can find the measurement series. This is the
        # basis for our query
        my $required = delete $obj->{'__required'};

        # Find any measurement documents that this might be impacting
        my $time;
        
        # If end for this message isn't defined, we need the current active one OR any that had an
        # end time that falls after the start time specified
        if (! defined $end){
            $time = {
                '$or' =>
                    [
                     {end => undef},
                     {end => {'$gte' => $start}}
                    ]
            };
        }
        # If end IS specified, we need to find an active one with a start time
        # earlier than this OR a no longer active one where the times are inside the range
        else {
            $time = {'$or' => [
                         {
                             '$and' => 
                                 [
                                  {end   => undef},
                                  {start => {'$lte' => $end}}
                                 ]
                         },
                         {
                             '$and' => 
                                 [
                                  {end => {'$gte' => $start}},
                                  {start => {'$lte' => $end}},
                                 ]
                         }
                         ]
            };
        }
               
        push(@$required, $time);        
        my $query = {'$and' => $required};

        log_debug("Query is: ", {filter => \&Data::Dumper::Dumper,
                                 value  => $query});

        my $measurements = $self->mongo_rw()->get_collection($type, 'measurements');

        my $cursor;
        eval {
            $cursor = $measurements->find($query)->sort({start => 1});
        };
        if ($@){
            $self->error("Error querying Mongo measurements: $@");
            return;
        }

        my @docs;
        while (my $doc = $cursor->next()){
            push(@docs, $doc);
        }

        log_debug("Found " . scalar(@docs) . " docs to update");

        # First record all of the times so we can see which ones need changing
        # if applicable

        # Keep track of where the next start point needs to be. Since the find above
        # is sorted by ascending start this allows us to grow/shrink each as necessary
        # in order. Start this off as the start of the update provided
        my $last_end = $start;

        # For each document we need to merge the existing metadata
        # in with the passed in metadata to see if anything
        # changed.
        log_debug("Remaining meta is " . Dumper($obj));
        
        for (my $i = 0; $i < @docs; $i++){
            my $doc = $docs[$i];

            # Keep a copy of the original doc for comparison
            my %original = %$doc;
            my $id       = $doc->{'identifier'};
            $self->_merge_meta_fields($obj, $doc) or return;

            # Make sure all the array fields are in the same order
            # before we compare them. This has no bearing on storage or 
            # anything else, just for comparison purposes
            $self->_meta_sort($doc) or return;
            $self->_meta_sort(\%original) or return;
            
            #warn "OLD IS " . Dumper(\%original);
            #warn "NEW IS " . Dumper($doc);
            
            my $is_same = Compare($doc, \%original, {ignore_hash_keys => ["identifier",
                                                                          "start", 
                                                                          "end", 
                                                                          "_id", 
                                                                          "last_updated"]});
            

            my $orig_end   = $original{'end'};
            my $orig_start = $original{'start'};
            my $orig_id    = $original{'_id'};
            
            # if the docs ended up being exactly the same, we can skip ahead
            # since there's nothing to do
            if ($is_same){
                log_debug("Skipping document since metadata is same");
                $last_end = $orig_end;
                next;
            }

            # This is a bit weird but makes later conditionals easier.
            # If the update has no "end" time set in it, we assume the
            # end is the same as the current doc's. The both undefined
            # case gets handled specifically.
            my $end_test = defined $end ? $end : $orig_end;

            log_debug("Creating a new version of metdata for $id");

            # There are a few cases here to reconcile the new data.
            my $now = time();
            $doc->{'last_updated'}    = $now;
            $original{'last_updated'} = $now;
            delete $doc->{'_id'}; # need to clear _id so Mongo autogen a new one

            # If this document is entirely surrounded by this new
            # metadata on both sides, we can just update it in place.
            # We can only do this replacement for non-active 
            # measurements since a change to an active measurement
            # makes it unactive 
            if (defined $orig_end && $orig_start >= $start && $orig_end <= $end_test){
                log_debug("Replacing doc from $orig_start to $orig_end entirely for $id");
                $measurements->update({_id => $orig_id}, $doc);
                $last_end = $orig_end;                
                $modified++;
            }

            # If this document is currently active, we need to decom
            # it and create a new version. This inherently is the last
            # update performed since the "no end" document is the current one
            elsif (! defined $orig_end){

                log_debug("Decomming current in service metadata for $id");

                # We can't do an update on start/end because they're part of the
                # sharding key, so we have to remove/insert. This is potentially
                # risky since the remove could succeed and the insert could fail
                # leaving us in a bad state. Mongo has no ACID or transactions or
                # anything so this is life I suppose.
                $measurements->remove({_id => $orig_id});
                $original{'end'}   = $last_end;

                # There's an edge case here that can cause a 0 duration document
                # to be generated - we can drop this instead since it's useless
                if ($original{'end'} != $original{'start'}){
                    $measurements->insert(\%original);
                }
                else {
                    log_debug("Omitting original due to 0 duration document");
                }
                
                $doc->{'end'}   = undef;
                $doc->{'start'} = $last_end;
                $measurements->insert($doc);
                $modified++;
            }
            # If this document DOES have an end AND it's not entirely
            # contained in the update, we need to fragment it
            else {

                my $fragged_left = 0;

                my %copy = %original;

                # Fragment on the left
                if ($orig_start <= $start){

                    log_debug("Fragment existing metadata document on left for $id");

                    if ($original{'end'} ne $start){
                        $original{'end'} = $start;
                        delete $original{'_id'};
                        $measurements->remove({_id => $orig_id});
                        
                        # Don't create 0 duration document
                        if ($original{'start'} != $original{'end'}){                        
                            $measurements->insert(\%original);
                            $modified++;
                        }
                        else {                    
                            log_debug("Skipping left original due to 0 duration document");
                        }
                    }

                    $doc->{'start'} = $start;

                    # Figure out what the stopping point for this
                    # fragment is
                    my $next_end;
                    if (defined $end && $orig_end < $end){
                        $next_end = $orig_end;
                    }
                    else {
                        $next_end = $end_test;
                    }
                    $doc->{'end'} = $next_end;

                    # There is an edge case where if we're going to create a measurement
                    # document of 0 duration, we can just skip it
                    if ($doc->{'start'} != $doc->{'end'}){
                        $measurements->insert($doc);
                        $fragged_left = 1;                        
                        $modified++;
                    }
                    else {
                        log_debug("Skipping left fragment due to 0 duration document");
                    }

                    $last_end = $next_end;
                }
                
                # If the previous one modified it, go back to the original data
                %original = %copy;

                # Fragment on the right - this can also happen with 
                # the left above so not elsif
                if (defined $end && $orig_end >= $end_test){

                    log_debug("Fragment existing metadata document on right for $id");
                    $original{'start'} = $last_end;
                    delete $original{'_id'};
                    $measurements->remove({_id => $orig_id});

                    # If we're not going to generate a 0 duration doc, go ahead and insert                    
                    if ($original{'start'} != $original{'end'}){
                        $measurements->insert(\%original);
                        $modified++;
                    }
                    else {
                        log_debug("Skipping right original due to 0 duration document");
                    }
                    
                    # If we fragmented on the left we don't need
                    # to double insert the new document
                    if (! $fragged_left){                        
                        $doc->{'start'} = $last_end;
                        $doc->{'end'}   = $orig_start;                        
                        
                        if ($doc->{'start'} != $doc->{'end'}){
                            $measurements->insert($doc);
                            $modified++;
                        }
                        else {
                            log_debug("Skipping right fragment due to 0 duration document");
                        }


                    }

                    $last_end = $orig_end;
                }
            }
        }
    }
    
    log_info("Modified $modified docs as a result of " . scalar(@$updates) . " update messages");

    return [{success => 1, modified => $modified}];
}

sub _merge_meta_fields {
    my $self = shift;
    my $new  = shift;
    my $old  = shift;

    foreach my $key (keys %$new){
        my $new_val = $new->{$key};
        
        # copy simple scalar
        if (! ref $new_val){
            $old->{$key} = $new_val;
        }

        # if it's a hash, recurse in to any subkeys
        elsif (ref $new_val eq 'HASH'){

            # If the original document didn't have this field, create a stub entry for it.
            # This might happen when a collector sends a record with barebones metadata
            # and this is updating it for the first time
            if (! exists $old->{$key}){
                $old->{$key} = {};
            }
            $self->_merge_meta_fields($new_val, $old->{$key});
        }

        # This one is hard... merging arrays. There's not really
        # a good way to figure out which element from $new would correspond (if at all)
        #  to elements in $old, so if the new one is sending in values we just assume
        # that it's a complete rewrite of the array. This can be suboptimal
        # in some cases, might have to come back to this to think of a better solution
        # if possible.
        elsif (ref $new_val eq 'ARRAY'){
            $old->{$key} = $new_val;
        }
    }

    return 1;
}

# Sort fields, this is really a helper function for update_measurement_metadata
# in order to get all the array fields in the same order so that a "diff" between
# two objects is the same time and time again
sub _meta_sort {
    my $self = shift;
    my $doc  = shift;

    foreach my $key (keys %$doc){
        my $val = $doc->{$key};
        my $ref = ref $val;
        
        # don't need to sort scalars
        next unless ($ref);
        
        if ($ref eq 'HASH'){
            $self->_meta_sort($val);
        }
        
        if ($ref eq 'ARRAY'){
            
            # Nothing to do if it's empty
            next if (@$val == 0);

            # This is kind of dangerous maybe. Assume that each
            # thing in the array is the same and has the same KVs
            my $first = $val->[0];

            # Very basic sanity check to ensure array has all the same
            # data types in it
            foreach my $el (@$val){
                if (ref $el ne ref $first){
                    $self->error("Not all values in array are of same type: hash, array, simple");
                    return;
                }
            }

            # If it's an array of simple scalars, just sort those
            if (! ref $first){
                my @sorted = sort @$val;
                $doc->{$key} = \@sorted;
            }
            # If it's an array of objects, find the keys of the
            # first object and sort based on that.
            elsif (ref $first eq 'HASH') {
                my @keys = sort keys %$first;

                # need to find a key that points to a simple scalar so
                # that the sorting is predictable
                my $chosen;                
                while (my $test = shift @keys){
                    if (! ref $first->{$test}){
                        log_debug("Choosing key \"$test\" as sort key for array");
                        $chosen = $test;
                        last;
                    }
                }

                if (! defined $chosen){
                    $self->error("Internal error: unable to find key to sort hash by - data structure too complex?");
                    return;
                }

                my @sorted = sort {$a->{$chosen} cmp $b->{$chosen}} @$val;
                $doc->{$key} = \@sorted;

                # Further sort if any fields require it
                foreach my $sorted (@sorted){
                    $self->_meta_sort($sorted);
                }
            }
            # Don't think we should ever get here. Having an array of 
            # arrays would be a broken data structure as far as this is concerned
            else {
                $self->error("Internal error: unknown data structure?");
                return;
            }    
        }    
    }

    return 1;
}

sub _verify_meta_fields {
    my $self      = shift;
    my $type      = shift;
    my $obj       = shift;
    my $meta      = shift;
    my $context   = shift || "";

    foreach my $obj_field (keys %$obj){            
        # make sure the field exists in the metadata
        if (! exists $meta->{$obj_field}){
            $self->error("Invalid metadata field \"$context$obj_field\" for type \"$type\"");
            return;
        }
        
        # if the metadata says this must be an array, make sure the values passed in 
        # are arrays
        my $is_array = 0;
        if (exists $meta->{$obj_field}{'array'} && $meta->{$obj_field}{'array'} eq 1){
            $is_array = 1;
            if (ref $obj->{$obj_field} ne 'ARRAY'){
                $self->error("Metadata for \"$obj_field\" must be an array of values.");
                return;
            }                
        }

        # Standardize format to array
        my $subs;
        if ($is_array){
            $subs = $obj->{$obj_field};
        }
        else {
            $subs = [$obj->{$obj_field}];
        }

        my $sub_metadata = $meta->{$obj_field}{'fields'};
        
        foreach my $sub_field (@$subs){        
            # if the metadata says this object has subfields, then we must ensure
            # that the object passed in is a hash
            if (exists $meta->{$obj_field}{'fields'}){
                if (ref $sub_field ne 'HASH'){
                    $self->error("Metadata for \"$obj_field\" must be an object with sub fields.");
                    return;
                }
                return if (! $self->_verify_meta_fields($type, $sub_field, $sub_metadata, "$obj_field."));
            }                            
        }        
    }

    return 1;
}


sub _update_measurement_metadata_sanity_check {
    my $self    = shift;
    my $updates = shift;

    if (ref $updates ne 'ARRAY'){
        $self->error("values must be an array of JSON objects");
        return;        
    }

    my %metadata_cache;

    foreach my $obj (@$updates){
        if (ref $obj ne 'HASH'){
            $self->error("values must be an array of JSON objects");
            return;
        }

        if (! exists $obj->{'type'}){
            $self->error("Message is missing required field \"type\" to indicate which type of measurement this is.");
            return;
        }


        my $type = $obj->{'type'};

        if (! $self->mongo_rw()->get_database($type)){
            $self->error("Error processing message for type \"$type\": " . $self->mongo_rw()->error());
            return;
        }

        my $metadata = $metadata_cache{$type};
        if (! $metadata){
            $metadata = $self->mongo_ro()->get_collection($type, 'metadata')->find_one();
            if (! defined $metadata){
                $self->error("Error getting metadata for type \"$type\": " . $self->mongo_ro()->error());
                return;
            }
            $metadata_cache{$type} = $metadata;
        }

        if (! exists $obj->{'start'} || $obj->{'start'} !~ /^\d+$/){
            $self->error("An object is missing or has invalid required field \"start\"");
            return;
        }

        # end must be present, but can be null
        if (! exists $obj->{'end'} || (defined $obj->{'end'} && $obj->{'end'} !~ /^\d+$/)){
            $self->error("An object is missing or has invalid required field \"end\"");
            return;
        }

        # make sure all the required fields were passed in, we have to be able to
        # uniquely identify each raw measurement series
        my $meta_fields = $metadata->{'meta_fields'};
        foreach my $field (keys %$meta_fields){  
            my $required = $meta_fields->{$field}{'required'} || 0; 
            if ($required){
                if (! exists $obj->{$field}){
                    $self->error("An object is missing required field \"$field\" for type \"$type\"");
                    return;                            
                }
                push(@{$obj->{'__required'}}, {$field => delete $obj->{$field}})
            }
        }


        my %copy = %$obj;
        delete $copy{'start'};
        delete $copy{'end'};
        delete $copy{'type'};
        delete $copy{'__required'};

        # make sure all the other fields were good
        return if (! $self->_verify_meta_fields($type, \%copy, $meta_fields));
    }

    return \%metadata_cache;
}

# DELETE METHODS
sub delete_measurement_types {
    my ( $self, %args ) = @_;

    my $measurement_type = $args{'name'};

    if(!defined($measurement_type)){
        $self->error( "Must include measurement type to delete");
        return;
    }

    my $data = $self->mongo_ro()->get_collection( $measurement_type, 'data' );
    if($data->find_one()){
        $self->error( "Can not delete measurement type, $measurement_type, as data collection is not empty");
        return;
    }
    $self->mongo_root()->get_database( $measurement_type, drop => 1) || return;

    return [{ 'success' => 1 }];
}

#TODO change method name to delete_value_fields
sub delete_measurement_type_values {
    my ( $self, %args ) = @_;

    my $measurement_type = $args{'measurement_type'};
    my $name             = $args{'name'};

    # sanity checks
    if(!defined($measurement_type)){
        $self->error( "Must include measurement type to delete");
        return;
    }
    if(!defined($name)){
        $self->error( "Must include the name of the value you wish to delete");
        return;
    }

    # get mongo classes
    my $db  = $self->mongo_rw()->get_database( $measurement_type );
    if(!$db){
        $self->error($self->mongo_rw()->error());
        return;
    }
    my $col = $db->get_collection( "metadata" );
    if(!$col){
        $self->error($self->mongo_rw()->error());
        return;
    }
   
    # remove the field 
    my $id = $col->update({}, { '$unset' => { "values.$name" => 1} } );
    if(!$id) {
        $self->error( "Error deleting value, $name in measurement type, $measurement_type");
        return;
    }

    return [{ 'success' => 1 }];
}

sub delete_meta_fields {
    my ( $self, %args ) = @_;
    
    my $measurement_type = $args{'measurement_type'};
    my $name             = $args{'name'};

    # sanity checks
    if(!defined($measurement_type)){
        $self->error( "Must include measurement type to delete");
        return;
    }
    if(!defined($name)){
        $self->error( "Must include the name of the meta field you wish to delete");
        return;
    }

    # get mongo classes
    my $db  = $self->mongo_rw()->get_database( $measurement_type );
    if(!$db){
        $self->error($self->mongo_rw()->error());
        return;
    }
    my $col = $db->get_collection( "metadata" );
    if(!$col){
        $self->error($self->mongo_rw()->error());
        return;
    }

    # verify meta_fields already exist and format name
    $name = $self->_format_meta_field_name( $name, collection => $col, exists => 1, not_required => 1 ) || return;

    # remove the field 
    my $id = $col->update({}, { '$unset' => { "meta_fields.$name" => 1}  } );
    if(!$id) {
        $self->error( "Error deleting meta field, $name in measurement type, $measurement_type");
        return;
    }

    return [{ 'success' => 1 }];
}

# HELPER METHODS

# returns the measurement_type prefix if string is a classifier otherwise just returns the string
sub _parse_measurement_type {
    my ($self, $string) = @_;
    return ($string =~ /(.*?)\.(.*)/) ? ($2,$1)  : (undef, $string);
}

sub _format_meta_field_name {
    my ($self, $name, %args) = @_;
    my $add          = $args{'add'};
    my $exists       = $args{'exists'};
    my $col          = $args{'collection'};
    my $not_required = $args{'not_required'};

    if(defined($exists) && !defined($col)){
        $self->error("You must pass in a collection to do an existence check");
        return;
    }

    # verify any parent meta fields already exists
    # and format name if need be
    my @meta_fields = split(/\./,$name);
    $name = ""; 
    for(my $i=0; $i<@meta_fields; $i++){
        my $meta_field = $meta_fields[$i];
        $name .= ($name eq "") ? $meta_field : ".fields.$meta_field";

        # if its an add and theres only 1 field left make sure the field doesn't already exists
        if( $add && ( ($i + 1) == @meta_fields ) ){
            my $cur = $col->find({ "meta_fields.".$name => { '$exists' => 1 }  });
            if( $exists && $cur->count() ){
                $self->error("Meta field, $name, already exists");
                return;
            }
            last;
        }

        # check that field exists
        my $cur = $col->find({ "meta_fields.".$name => { '$exists' => 1 }  });
        if( $exists && !$cur->count() ){
            $self->error("Meta field, $name, does not exist");
            return;
        }

        # make sure the field is not a required field
        $cur = $col->find({ "meta_fields.$name.required" => 1 });
        if($not_required && $cur->count()){
            $self->error("Meta field, $name, is required and therefore can not be deleted or have the array flag modified");
            return;
        }
    }

    return $name;
}

# helper function that filters out meta_fields based on some field values
sub _filter_fields {
    my ($self, $fields, %args) = @_;

    # if there were no filters passed in just return the fields
    return $fields if(!%args); 

    my $is_ordinal    = $args{'is_ordinal'}  || 0;
    my $is_required   = $args{'is_required'} || 0;
    my $is_classifier = $args{'is_classifier'} || 0;

    my $filtered_fields = [];
    foreach my $field (@$fields){
        next if($is_ordinal    && !$field->{'ordinal'});
        next if($is_required   && !$field->{'required'});
        next if($is_classifier && !$field->{'classifier'});

        # handle recursive case if their is subfields
        $field->{'fields'} = $self->_filter_fields( $field->{'fields'}, 
            is_ordinal  => $is_ordinal,
            is_required => $is_required
        ) if($field->{'fields'});


        push(@$filtered_fields, $field);
    }

    return $filtered_fields;
}

sub _sort_meta_fields_by_ordinal {
    my ($self, $meta_fields) = @_;

    # first sort this layer of the meta fields
    @$meta_fields = sort _by_ordinal( @$meta_fields );

    # now sort any subfields by their ordinal values
    foreach my $meta_field (@$meta_fields){
        #ooarce int fields to ints while looping through them
        $meta_field->{'ordinal'}  += 0 if(defined($meta_field->{'ordinal'}));
        $meta_field->{'required'} += 0 if(defined($meta_field->{'required'}));

        if($meta_field->{'fields'}){
            $meta_field->{'fields'} = $self->_sort_meta_fields_by_ordinal($meta_field->{'fields'});
        }
    } 

    return $meta_fields;
}

# sort by ordinal, putting the rows that lack an ordinal at the end
sub _by_ordinal {
    if (!exists($a->{'ordinal'}) || !exists($b->{'ordinal'})) {
        return exists($b->{'ordinal'}) - exists($a->{'ordinal'});
    }
    
    return $a->{'ordinal'} cmp $b->{'ordinal'};
}

sub _get_constraint_databases {
    my ( $self, %args ) = @_;

    my $constraints_file = $self->parser()->{'constraints_file'};

    if (defined($constraints_file)) {

        my $constraint_obj = GRNOC::TSDS::Constraints->new( config_file => $constraints_file );
        my $constraints = $constraint_obj->get_constraints();

        if (!defined($constraints) or @$constraints <= 0) {
            return;
        }

        my @databases = map { $_->{'database'} } @$constraints;

        return \@databases;
    }
    
    return;
}

1;
