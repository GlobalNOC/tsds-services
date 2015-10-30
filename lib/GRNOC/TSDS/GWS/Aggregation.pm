#--------------------------------------------------------------------
#----- GRNOC TSDS Aggregation GWS Library
#-----
#----- Copyright(C) 2013 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $LastChangedBy: $
#----- $LastChangedRevision: $
#----- $LastChangedDate: $
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/GWS/Aggregation.pm $
#----- $Id: Aggregation.pm 39282 2015-09-21 15:39:51Z bgeels $
#-----
#----- This module inherits the base GRNOC::TSDS::GWS class and
#----- provides all of the webservice methods to interact with the
#----- Aggregation DataService.
#--------------------------------------------------------------------

package GRNOC::TSDS::GWS::Aggregation;

use strict;
use warnings;

use base 'GRNOC::TSDS::GWS';

use GRNOC::TSDS::DataService::Aggregation;

use GRNOC::WebService::Method;
use GRNOC::WebService::Regex;
use GRNOC::WebService::Method::JIT;
use JSON qw( decode_json );

use Data::Dumper;

sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    my $self = $class->SUPER::new( @_ );

    bless( $self, $class );

    # get/store our data service
    $self->aggregation_ds( GRNOC::TSDS::DataService::Aggregation->new( @_ ) );

    return $self;
}

sub _init_methods {

    my $self = shift;

    my $method;

    # get_aggregations method
    $method = GRNOC::WebService::Method->new( name          => 'get_aggregations',
                                              description   => 'Gets a list of aggregations for a measurement type',
                                              expires       => '-1d',
                                              callback      => sub { $self->_get_aggregations( @_ ) } );

    # add the required measurement_type parameter to the get_aggregations() method
    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement type for which to get aggregations' );

    # register the get_aggregations() method
    $self->websvc()->register_method( $method );

    $method = GRNOC::WebService::Method->new( name          => 'get_expirations',
                                              description   => 'Gets a list of expirations for a measurement type',
                                              expires       => '-1d',
                                              callback      => sub { $self->_get_expirations( @_ ) } );

    # add the required measurement_type parameter to the get_expirations() method
    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement type for which to get expirations' );

    # register the get_expirations() method
    $self->websvc()->register_method( $method );

    # update_aggregations method
    $method = GRNOC::WebService::Method->new( name          => 'update_aggregations',
                                              description   => 'Updates aggregations for a measurement type',
                                              expires       => '-1d',
                                              callback      => sub { $self->_update_aggregations( @_ ) } );

    # add the required measurement_type parameter to the update_aggregations() method
    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement type for which to update aggregations' );

    # add the optional meta parameter to the update_aggregations() method
    $method->add_input_parameter( name          => 'meta',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The meta for which to update aggregations, a json blob containing the query to match' );

    # add the required name parameter to the update_aggregations() method
    $method->add_input_parameter( name          => 'name',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the aggregation to modify' );


    # add the optional meta_new parameter to the update_aggregations() method
    $method->add_input_parameter( name          => 'new_name',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The updated name' );

    # add the optional eval_position parameter to the update_aggregations() method
    $method->add_input_parameter( name          => 'eval_position',
                                  pattern       => $INTEGER,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The updated eval_order value to set for the expiration' );

    # add the optional values parameter to the update_aggregations() method
    $method->add_input_parameter( name          => 'values',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The updated histogram configuration for each value (note that changing this will require manually reaggregating)' );


    # register the update_aggregations() method
    $self->websvc()->register_method( $method );

    # update_expirations method
    $method = GRNOC::WebService::Method->new( name          => 'update_expirations',
                                              description   => 'Updates expirations for a measurement type',
                                              expires       => '-1d',
                                              callback      => sub { $self->_update_expirations( @_ ) } );

    # add the required measurement_type parameter to the update_expirations() method
    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement type for which to update expirations' );

    # add the optional meta parameter to the update_expirations() method
    $method->add_input_parameter( name          => 'meta',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The meta for which to update expirations, a json blob containing the query to match' );

    # add the required name parameter to the update_expirations() method
    $method->add_input_parameter( name          => 'name',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the expiration to modify' );


    # add the optional meta_new parameter to the update_expirations() method
    $method->add_input_parameter( name          => 'new_name',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The updated name' );

    # add the optional max_age parameter to the update_expirations() method
    $method->add_input_parameter( name          => 'max_age',
                                  pattern       => $INTEGER,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The updated max_age value to set for the expiration' );

    # add the optional eval_position parameter to the update_expirations() method
    $method->add_input_parameter( name          => 'eval_position',
                                  pattern       => $INTEGER,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The updated eval_order value to set for the expiration' );

    # register the update_expirations() method
    $self->websvc()->register_method( $method );

    # add_aggregation method
    $method = GRNOC::WebService::Method->new( name          => 'add_aggregation',
                                              description   => 'Adds an aggregation for a measurement type',
                                              expires       => '-1d',
                                              callback      => sub { $self->_add_aggregation( @_ ) } );

    # add the required measurement_type parameter to the add_aggregation() method
    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement type for which to insert aggregations' );

    # add the required interval to the add_aggregation() method
    $method->add_input_parameter( name          => 'interval',
                                  pattern       => $INTEGER,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The interval for which to insert aggregations' );

    # add the required meta parameter to the add_aggregation() method
    $method->add_input_parameter( name          => 'meta',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The meta for which to insert aggregations, a json blob containing the query to match' );

    # add the required name parameter to the add_aggregation() method
    $method->add_input_parameter( name          => 'name',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name for the new aggregation' );

    # add the required values parameter to the add_aggregation() method
    $method->add_input_parameter( name          => 'values',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The histogram configuration for each value for the new aggregation' );


    # register the add_aggregation() method
    $self->websvc()->register_method( $method );

    # add_expiration method
    $method = GRNOC::WebService::Method->new( name          => 'add_expiration',
                                              description   => 'Adds an expiration for a measurement type',
                                              expires       => '-1d',
                                              callback      => sub { $self->_add_expiration( @_ ) } );

    # add the required measurement_type parameter to the add_expiration() method
    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement type for which to insert expirations' );

    # add the required interval to the add_expiration() method
    $method->add_input_parameter( name          => 'interval',
                                  pattern       => $INTEGER,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The interval for which to insert expirations' );

    # add the required meta parameter to the add_expiration() method
    $method->add_input_parameter( name          => 'meta',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The meta for which to insert expirations, a json blob containing the query to match' );

    # add the required name parameter to the add_expiration() method
    $method->add_input_parameter( name          => 'name',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name for the new expiration' );

    # add the required max_age parameter to the add_expiration() method
    $method->add_input_parameter( name          => 'max_age',
                                  pattern       => $INTEGER,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The maximum age for the new expiration (in seconds)' );

    # register the add_expiration() method
    $self->websvc()->register_method( $method );

    # delete_aggregations method
    $method = GRNOC::WebService::Method->new( name          => 'delete_aggregations',
                                              description   => 'Delete aggregations for a measurement type.',
                                              expires       => '-1d',
                                              callback      => sub { $self->_delete_aggregations( @_ ) } );

    # add the required measurement_type parameter to the delete_aggregations() method
    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement type for which to delete aggregations' );

    # add the required name parameter to the delete_aggregations() method
    $method->add_input_parameter( name          => 'name',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the aggregation to delete' );

    # register the delete_aggregations() method
    $self->websvc()->register_method( $method );

    # delete_expirations method
    $method = GRNOC::WebService::Method->new( name          => 'delete_expirations',
                                              description   => 'Delete expirations for a measurement type.',
                                              expires       => '-1d',
                                              callback      => sub { $self->_delete_expirations( @_ ) } );

    # add the required measurement_type parameter to the delete_expirations() method
    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The measurement type for which to delete expirations' );

    # add the required name parameter to the delete_expirations() method
    $method->add_input_parameter( name          => 'name',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the expiration to delete' );

    # register the delete_expirations() method
    $self->websvc()->register_method( $method );

}

# callbacks

sub _get_aggregations {

    my ( $self, $method, $args ) = @_;

    my $results = $self->aggregation_ds()->get_aggregations( $self->process_args( $args ) );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->aggregation_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _get_expirations {

    my ( $self, $method, $args ) = @_;

    my $results = $self->aggregation_ds()->get_expirations( $self->process_args( $args ) );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->aggregation_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _update_aggregations {

    my ( $self, $method, $args ) = @_;

    my %processed = $self->process_args($args);

    # convert from JSON to hash
    if (exists $processed{'values'}){
	eval {
	    $processed{'values'} = decode_json($processed{'values'});
	};
	if ($@){
	    $method->set_error("Unable to decode values JSON: " . $@);
	    return;
	}
    }

    my $results = $self->aggregation_ds()->update_aggregations( %processed );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->aggregation_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _update_expirations {

    my ( $self, $method, $args ) = @_;

    my %processed = $self->process_args($args);
    my $results = $self->aggregation_ds()->update_expirations( %processed );

    # handle error
    if ( !$results ) {
        $method->set_error( $self->aggregation_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _add_aggregation {

    my ( $self, $method, $args ) = @_;

    # convert from JSON to hash
    my %processed = $self->process_args($args);
    if (exists $processed{'values'}){
        eval {
            $processed{'values'} = decode_json($processed{'values'});
        };
        if ($@){
            $method->set_error("Unable to decode values JSON: " . $@);
            return;
        }
    }
    my $results = $self->aggregation_ds()->add_aggregation( %processed );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->aggregation_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _add_expiration {

    my ( $self, $method, $args ) = @_;

    my %processed = $self->process_args($args);
    my $results = $self->aggregation_ds()->add_expiration( %processed );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->aggregation_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _delete_aggregations {

    my ( $self, $method, $args ) = @_;

    my $results = $self->aggregation_ds()->delete_aggregations( $self->process_args( $args ));

    # handle error
    if ( !$results ) {

        $method->set_error( $self->aggregation_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _delete_expirations {

    my ( $self, $method, $args ) = @_;

    my $results = $self->aggregation_ds()->delete_expirations( $self->process_args( $args ));

    # handle error
    if ( !$results ) {

        $method->set_error( $self->aggregation_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub update_constraints_file {

    my ( $self, $constraints_file ) = @_;

    $self->aggregation_ds->update_constraints_file($constraints_file);

}

1;

