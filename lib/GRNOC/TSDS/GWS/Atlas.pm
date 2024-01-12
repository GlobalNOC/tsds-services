#--------------------------------------------------------------------
#----- GRNOC TSDS Atlas GWS Library
#-----
#----- Copyright(C) 2014 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/GWS/Atlas.pm $
#----- $Id: Atlas.pm 38356 2015-07-24 16:05:28Z prattadi $
#-----
#----- This module inherits the base GRNOC::TSDS::GWS class.
#--------------------------------------------------------------------

package GRNOC::TSDS::GWS::Atlas;

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';
use lib './venv/lib/perl5';

use base 'GRNOC::TSDS::GWS';

use GRNOC::TSDS::DataService::Atlas;

use GRNOC::WebService::Method;
use GRNOC::WebService::Regex;

use Data::Dumper;

### constructor ###

sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    my $self = $class->SUPER::new( @_ );

    bless( $self, $class );

    # get/store our data service
    $self->atlas_ds( GRNOC::TSDS::DataService::Atlas->new( @_ ) );

    return $self;
}

### private methods ###

sub _init_methods {

    my ( $self ) = @_;

    my $method;

    $method = GRNOC::WebService::Method->new( name => 'get_atlas_data',
                                              description => 'Returns the data for the specified node+interfaces in an atlas-compatible XML format.',
                                              expires => '-1d',
                                              output_type => 'application/xml',
                                              output_formatter => sub { shift },
                                              callback => sub { $self->_get_atlas_data( @_ ) } );

    # add the required nodes parameter to the get_atlas_data() method
    $method->add_input_parameter( name => 'nodes',
                                  pattern => $NAME_ID,
                                  required => 1,
                                  multiple => 0,
                                  description => 'A comma-separted list of nodes which is combined with the other comma-separated list of interfaces given.' );

    # add the required interfaces parameter to the get_atlas_data() method
    $method->add_input_parameter( name => 'interfaces',
                                  pattern => $NAME_ID,
                                  required => 1,
                                  multiple => 0,
                                  description => 'A comma-separted list of interfaces which is combined with the other comma-separated list of nodes given.' );

    # register the get_atlas_data() method
    $self->websvc()->register_method( $method );

    $method = GRNOC::WebService::Method->new( name => 'make_atlas_graph',
					      description => 'Returns a PNG image based upon the options provided, suitable for embedding an a webpage or email.',
					      expires => '-1d',
					      output_type => 'image/png',
					      output_formatter => sub { shift },
					      callback => sub { $self->_make_atlas_graph( @_ ) } );

    # add the required node_name parameter to the make_atlas_graph() method
    $method->add_input_parameter( name => 'node_name',
				  pattern => $NAME_ID,
				  required => 1,
				  multiple => 0,
				  description => 'The name of the node to generate the graph for.' );

    # add the required int_name parameter to the make_atlas_graph() method
    $method->add_input_parameter( name => 'int_name',
				  pattern => $NAME_ID,
				  required => 1,
				  multiple => 0,
				  description => 'The name of the interface to generate the graph for.' );

    # add the required min parameter to the make_atlas_graph() method
    $method->add_input_parameter( name => 'min',
				  pattern => $NUMBER_ID,
				  required => 1,
				  multiple => 0,
				  description => 'The duration, in minutes, of the graph to generate.' );

    # add the required width parameter to the make_atlas_graph() method
    $method->add_input_parameter( name => 'width',
				  pattern => $NUMBER_ID,
				  required => 1,
				  multiple => 0,
				  description => 'The width, in pixels, of the graph to generate.' );

    # add the required height parameter to the make_atlas_graph() method
    $method->add_input_parameter( name => 'height',
				  pattern => $NUMBER_ID,
				  required => 1,
				  multiple => 0,
				  description => 'The height, in pixels, of the graph to generate.' );

    # add the optional bgcolor parameter to the make_atlas_graph() method
    $method->add_input_parameter( name => 'bgcolor',
				  pattern => '^([a-fA-F0-9]{6})$',
				  required => 0,
				  multiple => 0,
				  default => 'F3F3F3',
				  description => 'The hex color code of the background to generate for the graph.' );

    # add the optional timezone parameter to the make_atlas_graph() method
    $method->add_input_parameter( name => 'timezone',
				  pattern => $NAME_ID,
				  required => 0,
				  multiple => 0,
				  default => 'UTC',
				  description => 'The IANA name of the timezone to use in the graph.' );

    # register the make_atlas_graph() method
    $self->websvc()->register_method( $method );    
}

### callbacks ###

sub _get_atlas_data {

    my ( $self, $method, $args ) = @_;

    my $results = $self->atlas_ds()->get_atlas_data( $self->process_args( $args ) );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->atlas_ds()->error() );
        return;
    }

    return $results;
}

sub _make_atlas_graph {

    my ( $self, $method, $args ) = @_;

    my $results = $self->atlas_ds()->make_atlas_graph( $self->process_args( $args ) );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->atlas_ds()->error() );
        return;
    }

    return $results;
}

sub update_constraints_file {

    my ( $self, $constraints_file ) = @_;

    $self->atlas_ds->update_constraints_file($constraints_file);

}

1;
