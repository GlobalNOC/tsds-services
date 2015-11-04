#--------------------------------------------------------------------
#----- GRNOC TSDS Admin GWS Library
#-----
#----- Copyright(C) 2015 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/GWS/Admin.pm $
#----- $Id: Admin.pm 39384 2015-09-24 19:19:22Z mrmccrac $
#-----
#----- This module inherits the base GRNOC::TSDS::GWS class and
#----- provides all of the webservice methods to interact with the
#----- Admin DataService.
#--------------------------------------------------------------------

package GRNOC::TSDS::GWS::Admin;

use strict;
use warnings;

use base 'GRNOC::TSDS::GWS';

use GRNOC::TSDS::DataService::MetaData;

use GRNOC::WebService::Method;
use GRNOC::WebService::Regex;

use Data::Dumper;

sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    my $self = $class->SUPER::new( @_ );

    bless( $self, $class );

    # get/store our data service
    $self->metadata_ds( GRNOC::TSDS::DataService::MetaData->new( @_ ) );

    return $self;
}

sub _init_methods {

    my $self = shift;

    my $method;

    # ADD METHODS
    $method = GRNOC::WebService::Method->new( 
        name          => 'add_measurement_type',
        description   => 'Adds a measurement types',
        expires       => '-1d',
        callback      => sub { $self->_add_measurement_type( @_ ) }
    );
    
    $method->add_input_parameter( name          => 'name',
                                  pattern       => '^([a-z|\_]+)$',
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the measurement type' );

    $method->add_input_parameter( name          => 'label',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The label displayed in the fronted for the measurement_type' );
    
    $method->add_input_parameter( name          => 'required_meta_field',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 1,
                                  description   => 'The required meta fields of the measurement_type' );

    $method->add_input_parameter( name          => 'search_weight',
                                  pattern       => $NUMBER_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'A positive "search weight" integer that determines the order in which measurement type search results are returned' );

    $method->add_input_parameter( name          => 'ignore_si',
                                  pattern       => '^([1|0])$',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Boolean (1|0) to determine if SI notation should be used.' );
    
    $self->websvc()->register_method( $method );
    
    $method = GRNOC::WebService::Method->new( 
        name          => 'add_measurement_type_value',
        description   => 'Adds a new value to be measured to a measurement type',
        expires       => '-1d',
        callback      => sub { $self->_add_measurement_type_value( @_ ) }
    );
    
    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement_type this value should be added to' );
    
    $method->add_input_parameter( name          => 'name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the value' );
    
    $method->add_input_parameter( name          => 'description',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The description displayed in the frontend of the value' );
    
    $method->add_input_parameter( name          => 'units',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The units of the value being collected' );
    
    $method->add_input_parameter( name          => 'ordinal',
                                  pattern       => $INTEGER,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The ordinal integer indicating the order it will be displayed in the frontend' );

    $self->websvc()->register_method( $method );
    
    $method = GRNOC::WebService::Method->new( 
        name          => 'add_meta_field',
        description   => "Adds a new meta field to a measurement type",
        expires       => '-1d',
        callback      => sub { $self->_add_meta_field( @_ ) }
    );

    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => '^([a-z|\_]+)$',
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement_type this value should be added to' );

    $method->add_input_parameter( name          => 'name',
                                  pattern       => '^([a-z|\_|\.]+)$',
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the value' );

    $method->add_input_parameter( name          => 'array',
                                  pattern       => '^(0|1)$',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Flag indicating whether or not this value is an array' );
    
    $method->add_input_parameter( name          => 'classifier',
                                  pattern       => '^(0|1)$',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Flag indicating whether or not this meta field should be treated as a primary value and made browsable in the frontend' );

    $method->add_input_parameter( name          => 'ordinal',
                                  pattern       => $INTEGER,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The ordinal integer indicating the order it will be displayed in the frontend' );

    $method->add_input_parameter( name          => 'search_weight',
                                  pattern       => $NUMBER_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'A positive "search weight" integer indicating the order items will be returned from the search service.' );

    $self->websvc()->register_method( $method );


    # UPDATE METHODS
    $method = GRNOC::WebService::Method->new( 
        name          => 'update_measurement_types',
        description   => "Updates specified measurement types",
        expires       => '-1d',
        callback      => sub { $self->_update_measurement_types( @_ ) }
    );

    $method->add_input_parameter( name          => 'name',
                                  pattern       => '^([a-z|\_]+)$',
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the measurement_type' );
    
    $method->add_input_parameter( name          => 'label',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The label of the measurement_type' );
    
    $method->add_input_parameter( name          => 'data_doc_limit',
                                  pattern       => $NUMBER_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'A system safeguard that sets the maximum number of mongo documents a query can match for this measurement_type' );
    
    $method->add_input_parameter( name          => 'event_limit',
                                  pattern       => $NUMBER_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'A system safeguard that sets the maximum number of events a query can return for this measurement_type' );

    $method->add_input_parameter( name          => 'search_weight',
                                  pattern       => $NUMBER_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'A positive "search weight" integer that determines the order in which measurement type search results are returned' );

    $method->add_input_parameter( name          => 'ignore_si',
                                  pattern       => '^([1|0])$',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Boolean (1|0) to determine if SI notation should be used.' );


    $self->websvc()->register_method( $method );
    
    $method = GRNOC::WebService::Method->new( 
        name          => 'update_measurement_type_values',
        description   => "Updates specified measurement types' values",
        expires       => '-1d',
        callback      => sub { $self->_update_measurement_type_values( @_ ) }
    );

    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement_type of the value to be updated' );

    $method->add_input_parameter( name          => 'name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the value to be modified' );

    $method->add_input_parameter( name          => 'description',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The description displayed in the frontend of the value' );

    $method->add_input_parameter( name          => 'units',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The units of the value being collected' );

    $method->add_input_parameter( name          => 'ordinal',
                                  pattern       => $INTEGER,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The ordinal integer indicating the order it will be displayed in the frontend' );

    $self->websvc()->register_method( $method );
    
    $method = GRNOC::WebService::Method->new( 
        name          => 'update_meta_fields',
        description   => "Updates specified measurement types' meta field",
        expires       => '-1d',
        callback      => sub { $self->_update_meta_fields( @_ ) }
    );

    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => '^([a-z|\_]+)$',
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement_type of the meta field to be updated' );

    $method->add_input_parameter( name          => 'name',
                                  pattern       => '^([a-z|\_]+)$',
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the meta field to be updated' );

    $method->add_input_parameter( name          => 'array',
                                  pattern       => '^(0|1)$',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Flag indicating whether or not this value is an array' );

    $method->add_input_parameter( name          => 'classifier',
                                  pattern       => '^(0|1)$',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Flag indicating whether or not this meta field should be treated as a primary value and made browsable in the frontend' );

    $method->add_input_parameter( name          => 'ordinal',
                                  pattern       => $INTEGER,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The ordinal integer indicating the order it will be displayed in the frontend' );

    $method->add_input_parameter( name          => 'search_weight',
                                  pattern       => $NUMBER_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'A positive "search weight" integer indicating the order items will be returned from the search service.' );


    $self->websvc()->register_method( $method );


    $method = GRNOC::WebService::Method->new( 
        name          => 'update_measurement_metadata',
        description   => "Updates one or more measurements' metadata. Values are sent as a JSON encoded array of objects, each representing the values and timeframe for one measurement. Existing values in the metadata documents are merged in with values passed to determine what needs to happen, so updating one or N fields is easily possible. An example of updating the circuit name for a particular interface might look like

[
{
  type: \"interface\",
  node: \"rtr.foo.bar\",
  intf: \"ae0\",
  start: 1442241179,
  end: null,
  circuit: [{
      name: \"IL-10GE-FOO-BAR-1234\"    
  }]
}
]",

        expires       => '-1d',
        callback      => sub { $self->_update_measurement_metadata( @_ ) }
    );

    $method->add_input_parameter( name          => 'values',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The array of JSON encoded data.');

    $self->websvc()->register_method( $method );

    # DELETE METHODS
    $method = GRNOC::WebService::Method->new( 
        name          => 'delete_measurement_types',
        description   => "Delete specified measurement type",
        expires       => '-1d',
        callback      => sub { $self->_delete_measurement_types( @_ ) }
    );
    
    $method->add_input_parameter( name          => 'name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the measurement_type' );

    $self->websvc()->register_method( $method );
    
    $method = GRNOC::WebService::Method->new( 
        name          => 'delete_measurement_type_values',
        description   => 'Deletes a value from the specified measurement type',
        expires       => '-1d',
        callback      => sub { $self->_delete_measurement_type_values( @_ ) }
    );

    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement_type of the meta field to be deleted' );

    $method->add_input_parameter( name          => 'name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the meta field to be deleted' );

    $self->websvc()->register_method( $method );
    
    $method = GRNOC::WebService::Method->new( 
        name          => 'delete_meta_fields',
        description   => 'Deletes a meta field from the specified measurement type',
        expires       => '-1d',
        callback      => sub { $self->_delete_meta_fields( @_ ) }
    );

    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement_type of the meta field to be deleted' );

    $method->add_input_parameter( name          => 'name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the meta field to be deleted' );

    $self->websvc()->register_method( $method );


}

# ADD CALLBACKS
sub _add_measurement_type {
    my ( $self, $method, $args ) = @_;
    
    my $results = $self->metadata_ds()->add_measurement_type( $self->process_args( $args ));

    # handle error
    if ( !$results ) {
        $method->set_error( $self->metadata_ds()->error() );
        return;
    }
    return { results => $results }; 
}

sub _add_measurement_type_value {
    my ( $self, $method, $args ) = @_;
    
    my $results = $self->metadata_ds()->add_measurement_type_value( $self->process_args( $args ));

    # handle error
    if ( !$results ) {
        $method->set_error( $self->metadata_ds()->error() );
        return;
    }
    return { results => $results }; 
}

sub _add_meta_field {
    my ( $self, $method, $args ) = @_;
    
    my $results = $self->metadata_ds()->add_meta_field( $self->process_args( $args ));

    # handle error
    if ( !$results ) {
        $method->set_error( $self->metadata_ds()->error() );
        return;
    }
    return { results => $results }; 
}

# UPDATE CALLBACKS
sub _update_measurement_types {
    my ( $self, $method, $args ) = @_;
    my $results = $self->metadata_ds()->update_measurement_types( $self->process_args( $args ));

    # handle error
    if ( !$results ) {
        $method->set_error( $self->metadata_ds()->error() );
        return;
    }
    return { results => $results }; 
}

sub _update_measurement_type_values {
    my ( $self, $method, $args ) = @_;
    
    my $results = $self->metadata_ds()->update_measurement_type_values( $self->process_args( $args ));

    # handle error
    if ( !$results ) {
        $method->set_error( $self->metadata_ds()->error() );
        return;
    }
    return { results => $results }; 
}

sub _update_meta_fields {
    my ( $self, $method, $args ) = @_;
    
    my $results = $self->metadata_ds()->update_meta_fields( $self->process_args( $args ));

    # handle error
    if ( !$results ) {
        $method->set_error( $self->metadata_ds()->error() );
        return;
    }
    return { results => $results }; 
}


sub _update_measurement_metadata {
    my ( $self, $method, $args ) = @_;

    my $vals;

    eval {
        $vals = JSON::decode_json($args->{'values'}{'value'});
    };
    if ($@){
        $method->set_error("Unable to decode JSON values: $@");
        return;
    }


    my $results = $self->metadata_ds()->update_measurement_metadata( values => $vals );

    # handle error
    if ( !$results ) {
        $method->set_error( $self->metadata_ds()->error() );
        return;
    }
    return { results => $results }; 
}

# DELETE CALLBACKS
sub _delete_measurement_types {
    my ( $self, $method, $args ) = @_;
    
    my $results = $self->metadata_ds()->delete_measurement_types( $self->process_args( $args ));

    # handle error
    if ( !$results ) {
        $method->set_error( $self->metadata_ds()->error() );
        return;
    }
    return { results => $results }; 
}

sub _delete_measurement_type_values {
    my ( $self, $method, $args ) = @_;
    
    my $results = $self->metadata_ds()->delete_measurement_type_values( $self->process_args( $args ));

    # handle error
    if ( !$results ) {
        $method->set_error( $self->metadata_ds()->error() );
        return;
    }
    return { results => $results }; 
}

sub _delete_meta_fields {
    my ( $self, $method, $args ) = @_;
    
    my $results = $self->metadata_ds()->delete_meta_fields( $self->process_args( $args ));

    # handle error
    if ( !$results ) {
        $method->set_error( $self->metadata_ds()->error() );
        return;
    }
    return { results => $results }; 
}

1;

