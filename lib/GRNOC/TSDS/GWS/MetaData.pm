#--------------------------------------------------------------------
#----- GRNOC TSDS MetaData GWS Library
#-----
#----- Copyright(C) 2015 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/GWS/MetaData.pm $
#----- $Id: MetaData.pm 39609 2015-10-07 13:41:29Z bgeels $
#-----
#----- This module inherits the base GRNOC::TSDS::GWS class and
#----- provides all of the webservice methods to interact with the
#----- MetaData DataService.
#--------------------------------------------------------------------

package GRNOC::TSDS::GWS::MetaData;

use strict;
use warnings;

use base 'GRNOC::TSDS::GWS';

use GRNOC::TSDS::DataService::MetaData;

use GRNOC::WebService::Method;
use GRNOC::WebService::Regex;
use GRNOC::WebService::Method::JIT;

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

    $method = GRNOC::WebService::Method->new( name          => 'get_measurement_types',
                                              description   => 'Returns a unique list of all possible measurement types.',
                                              expires       => '-1d',
                                              callback      => sub { $self->_get_measurement_types( @_ ) } );
    
    $method->add_input_parameter( name          => 'show_measurement_count',
                                  pattern       => '^(1|0)$',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Flag that indicates whether or not to include the count of measurements for the measurement_type' );
    
    $method->add_input_parameter( name          => 'show_classifiers',
                                  pattern       => '^(1|0)$',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Flag that indicates whether or not to include classifiers in the results' );
    
    $method->add_input_parameter( name          => 'show_required_fields',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Flag that indicates whether or not to include the required fields for the measurement type' );

    # register the get_measurement_types() method
    $self->websvc()->register_method( $method );

    $method = GRNOC::WebService::Method->new( name          => 'get_measurement_type_schemas',
                                              description   => 'Return a hash lookup containing meta information for all the measurement_types. Should be used by interfacing clients/apps to determine what to display by default for a measurement_type as well as what\'s available to be displayed.',
                                              expires       => '-1d',
                                              callback      => sub { $self->_get_measurement_type_schemas( @_ ) } );

    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => '^([a-z|\_\.]+)$',
                                  required      => 0,
                                  multiple      => 1,
                                  description   => 'Constrains schema to measurement_types passed in. Otherwise all measurement_types or included.');
    
    $method->add_input_parameter( name          => 'flatten_fields',
                                  pattern       => '^(1|0)$',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Flattens the list of value and meta fields for each measurement type\'s schema' );

    # register the get_measurement_types() method
    $self->websvc()->register_method( $method );

    $method = GRNOC::WebService::Method->new( name          => 'get_meta_fields',
                                              description   => 'Gets a unique list of meta field keys.',
                                              expires       => '-1d',
                                              callback      => sub { $self->_get_meta_fields( @_ ) } );

    # add the required measurement_type parameter to the get_meta_fields() method
    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => '^([a-z|\_\.]+)$',
                                  required      => 1,
                                  multiple      => 0,
                                  description   => '' );
    
    # add the required measurement_type parameter to the get_meta_fields() method
    $method->add_input_parameter( name          => 'is_ordinal',
                                  pattern       => '^(1|0)$',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Flag to indicate whether to only show meta fields with an ordinal value set' );
    
    # add the required measurement_type parameter to the get_meta_fields() method
    $method->add_input_parameter( name          => 'is_required',
                                  pattern       => '^(1|0)$',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Flag to indicate whether to only show meta fields with an required value set' );

    # add the optional meta_field parameter to the get_meta_fields() method
    $method->add_input_parameter( name          => 'meta_field',
                                  pattern       => '^([a-z|\_]+)$',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Show only the meta fields for a specific classifier within the passed in measurement_type (deprecated: should do this by passing in a measurement_type value strucutured like so $measurment_type.$classifier")' );

    # register the get_meta_fields() method
    $self->websvc()->register_method( $method );   

    # static parameter of get_meta_field_values()
    my $static_parameters = [{
        name        => 'measurement_type',
        required    => 1,
        multiple    => 0,
        pattern     => '^([a-z|\_\.]+)$',
        description => ""
    },{
        name        => 'limit',
        required    => 1,
        multiple    => 0,
        pattern     => $NUMBER_ID,
        description => 'The maximum number of results to show'
    },{
        name        => 'offset',
        required    => 1,
        multiple    => 0,
        pattern     => $NUMBER_ID,
        description => 'What offset to use when getting the list of values'
    }];


    $method = GRNOC::WebService::Method::JIT->new( 
         name                             => 'get_meta_field_values',
         description                      => 'Gets all the values currently set on the meta_field(s).',
         expires                          => '-1d',
         static_input_parameters          => $static_parameters,
         dynamic_input_parameter_callback => sub { 
            $self->_get_meta_field_values_dynamic_parameters(@_);
         },  
         callback                         => sub { 
            $self->_get_meta_field_values( @_ ) 
         } 
    );

    $self->websvc()->register_method( $method );

    # static parameter of get_distinct_meta_field_values()
    $static_parameters = [{
        name        => 'measurement_type',
        required    => 1,
        multiple    => 0,
        pattern     => '^([a-z|\_\.]+)$',
        description => ""
    },{
        name        => 'limit',
        required    => 1,
        multiple    => 0,
        pattern     => $NUMBER_ID,
        description => 'The maximum number of results to show'
    }];


    $method = GRNOC::WebService::Method::JIT->new(
         name                             => 'get_distinct_meta_field_values',
         description                      => 'Gets all the values currently set on the meta_field(s).',
         expires                          => '-1d',
         static_input_parameters          => $static_parameters,
         dynamic_input_parameter_callback => sub {
            $self->_get_meta_field_values_dynamic_parameters(@_);
         },
         callback                         => sub {
            $self->_get_distinct_meta_field_values( @_ )
         }
    );

    $self->websvc()->register_method( $method );

    $method = GRNOC::WebService::Method->new( name          => 'get_measurement_type_values',
                                              description   => 'Gets a unique list of measurement type values.',
                                              expires       => '-1d',
                                              callback      => sub { $self->_get_measurement_type_values( @_ ) } );

    # add the required measurement_type parameter to the get_measurement_type_values() method
    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => '^([a-z|\_\.]+)$',
                                  required      => 1,
                                  multiple      => 0,
                                  description   => '' );
 
    # register the get_measurement_type_values() method
    $self->websvc()->register_method( $method );

}

# callbacks

sub _get_measurement_types {

    my ( $self, $method, $args ) = @_;

    my $results = $self->metadata_ds()->get_measurement_types( $self->process_args( $args ) );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->metadata_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _get_measurement_type_schemas {

    my ( $self, $method, $args ) = @_;

    my $results = $self->metadata_ds()->get_measurement_type_schemas( $self->process_args( $args ) );

    # handle error
    if ( !$results ) {
        $method->set_error( $self->metadata_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _get_meta_fields {

    my ( $self, $method, $args ) = @_;

    my $results = $self->metadata_ds()->get_meta_fields( $self->process_args( $args ));

    # handle error
    if ( !$results ) {

        $method->set_error( $self->metadata_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _get_measurement_type_values {

    my ( $self, $method, $args ) = @_;

    my $results = $self->metadata_ds()->get_measurement_type_values( $self->process_args( $args ));

    # handle error
    if ( !$results ) {

        $method->set_error( $self->metadata_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _get_meta_field_values {

    my ( $self, $method, $args ) = @_;

    my $results = $self->metadata_ds()->get_meta_field_values( $self->process_args( $args ));

    # handle error
    if ( !$results ) {

        $method->set_error( $self->metadata_ds()->error() );
        return;
    }

    return {
	    total   => $self->metadata_ds()->parser()->total(),
	    results => $results
    };
}

sub _get_distinct_meta_field_values {
    my ( $self, $method, $args ) = @_;

    my $results = $self->metadata_ds()->get_distinct_meta_field_values( $self->process_args( $args ));

    # handle error
    if ( !$results ) {

        $method->set_error( $self->metadata_ds()->error() );
        return;
    }

    return {
	total   => scalar(@$results),
	results => $results
    };
}

sub _get_meta_field_values_dynamic_parameters {

    my ( $self, $method, $args ) = @_;

    my $measurement_type = $args->{'measurement_type'}{'value'} || undef;

    unless ($measurement_type){
        
        return;
    }

    my $common_parameters = [
                             {name => 'meta_field',
                              required =>1,
                              multiple =>0,
                              pattern => '^([a-z|\_\.]+)$',
                              description => "",
                             }
                            ];

    my $results = $self->metadata_ds()->get_meta_fields(measurement_type => $measurement_type);

    my $new_parameters = $self->_get_meta_field_values_dynamic_parameters_helper($results, [], "");

    return if (! defined $results);

    push(@$new_parameters, @$common_parameters);

    return $new_parameters;
}

sub _get_meta_field_values_dynamic_parameters_helper {
    my $self       = shift;
    my $results    = shift;
    my $parameters = shift;
    my $prefix     = shift;

    return $parameters if (ref $results ne 'ARRAY');

    foreach my $field  (@$results){

	# does this have sub fields?
	if ($field->{'fields'}){
	    $parameters = $self->_get_meta_field_values_dynamic_parameters_helper($field->{'fields'},
										  $parameters,
										  $prefix . $field->{'name'} . ".");
	}
	else {
	    push (@$parameters,
		  {
		      name                => $prefix . $field->{'name'},
		      pattern             => $TEXT,
		      required            => 0,
		      multiple            => 0,
		      description         => "Dynamic filter field",
		      add_logic_parameter => 1
		  }
		);
	}
    }

    return $parameters;
}

sub update_constraints_file {

    my ( $self, $constraints_file ) = @_;

    $self->metadata_ds->update_constraints_file($constraints_file);

}

1;

