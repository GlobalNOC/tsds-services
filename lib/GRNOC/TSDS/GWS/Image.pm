#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
#--------------------------------------------------------------------
#----- GRNOC TSDS Image GWS Library
#-----
#----- Copyright(C) 2013 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $LastChangedBy: $
#----- $LastChangedRevision: $
#----- $LastChangedDate: $
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/GWS/Image.pm $
#----- $Id: Image.pm 35857 2015-03-10 22:06:04Z prattadi $
#-----
#----- This module inherits the base GRNOC::TSDS::GWS class and
#----- provides all of the webservice methods to interact with the
#----- Image DataService.
#--------------------------------------------------------------------

package GRNOC::TSDS::GWS::Image;

use strict;
use warnings;

use base 'GRNOC::TSDS::GWS';

use GRNOC::TSDS::DataService::Image;

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
    $self->image_ds( GRNOC::TSDS::DataService::Image->new( @_ ) );

    return $self;
}

sub _init_methods {

    my $self = shift;

    my $method;

    $method = GRNOC::WebService::Method->new( name          => 'get_image',
                                              description   => 'Returns an image for given SVGs.',
                                              expires       => '-1d',
                                              callback      => sub { $self->_get_image( @_ ) } );

    # add the required svg parameter to the get_image() method
    $method->add_input_parameter( name                              => 'content',
                                  pattern                           => '^((\n|.|\/)+)$',
                                  ignore_default_input_validators   => 1,
                                  required                          => 1,               
                                  description                       => 'The HTML to render as an image.' );


    # register the get_image() method
    $self->websvc()->register_method( $method );

    $method = GRNOC::WebService::Method->new( name          => 'get_chart',
                                              description   => 'Returns a chart.',
                                              expires       => '-1d',
                                              callback      => sub { $self->_get_chart( @_ ) } );

    # add the required query parameter to the get_chart() method
    $method->add_input_parameter( name                              => 'query',
                                  pattern                           => '^((\n|.|\/)+)$',
                                  ignore_default_input_validators   => 1,
                                  required                          => 1,               
                                  description                       => 'The query for creating a chart.' );

    # add the optional output_format parameter to the get_chart() method
    $method->add_input_parameter( name                              => 'output_format',
                                  pattern                           => '^((\n|.|\/)+)$',
                                  ignore_default_input_validators   => 1,
                                  required                          => 0,               
                                  description                       => 'Output format - binary or base64' );

    # add the optional type parameter to the get_chart() method
    $method->add_input_parameter( name                              => 'type',
                                  pattern                           => '^((\n|.|\/)+)$',
                                  ignore_default_input_validators   => 1,
                                  required                          => 0,
                                  description => 'The chart type' );

    # register the get_chart() method
    $self->websvc()->register_method( $method );
}

# callbacks

sub _get_image {

    my ( $self, $method, $args ) = @_;

    my $results = $self->image_ds()->get_image($self->process_args( $args ));

    # handle error
    if ( !$results ) {

        $method->set_error( $self->image_ds()->error() );
        return;
    }

    return {
        results => [{image => $results}],
    };
}

sub _get_chart {

    my ( $self, $method, $args ) = @_;

    my $results = $self->image_ds()->get_chart($self->process_args( $args ));

    # handle error
    if ( !$results ) {

        $method->set_error( $self->image_ds()->error() );
        return;
    }

    return {
        results => [{chart => $results}],
    };
}
1;

