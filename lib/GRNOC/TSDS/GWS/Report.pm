#--------------------------------------------------------------------
#----- GRNOC TSDS Report GWS Library
#-----
#----- Copyright(C) 2013 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $LastChangedBy: $
#----- $LastChangedRevision: $
#----- $LastChangedDate: $
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/GWS/Report.pm $
#----- $Id: Report.pm 38651 2015-08-12 20:07:10Z prattadi $
#-----
#----- This module inherits the base GRNOC::TSDS::GWS class and
#----- provides all of the webservice methods to interact with the
#----- Report DataService.
#--------------------------------------------------------------------

package GRNOC::TSDS::GWS::Report;

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';
use lib './venv/lib/perl5';

use base 'GRNOC::TSDS::GWS';

use GRNOC::TSDS::DataService::Report;

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
    $self->report_ds( GRNOC::TSDS::DataService::Report->new( @_ ) );

    return $self;
}

sub _init_methods {

    my $self = shift;

    my $method;

    $method = GRNOC::WebService::Method->new( name          => 'get_reports',
                                              description   => 'Get all the reports available',
                                              expires       => '-1d',
                                              callback      => sub { $self->_get_reports( @_ ) } );

    # add the optional type parameter to the get_reports() method
    $method->add_input_parameter( name          => 'type',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 1,
                                  description   => '' );

    # add the optional order_by parameter to the get_reports() method
    $method->add_input_parameter( name          => 'order_by',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  default       => 'name',
                                  description   => 'Which field to use to order the results');

    # add the optional order parameter to the get_reports() method
    $method->add_input_parameter( name          => 'order',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  default       => 'ASC',
                                  description   => 'Whether to sort ascending (ASC) or descending (DESC)' );

    # add the optional offset parameter to the get_reports() method
    $method->add_input_parameter( name          => 'offset',
                                  pattern       => $INTEGER,
                                  required      => 0,
                                  multiple      => 0,
                                  default       => '0',
                                  description   => 'Offset for pagination' );
    # add the optional limit parameter to the get_reports() method
    $method->add_input_parameter( name          => 'limit',
                                  pattern       => $INTEGER,
                                  required      => 0,
                                  multiple      => 0,
                                  default       => '0',
                                  description   => 'Page size for paginated results' );

    # register the get_reports() method
    $self->websvc()->register_method( $method );


    $method = GRNOC::WebService::Method->new( name          => 'get_report_details',
                                              description   => 'Get Report Details',
                                              expires       => '-1d',
                                              callback      => sub { $self->_get_report_details( @_ ) } );

    # add the required name parameter to the get_report_details() method
    $method->add_input_parameter( name          => 'name',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => '' );

    # register the get_report_details() method
    $self->websvc()->register_method( $method );

    $method = GRNOC::WebService::Method->new( name          => 'get_report_template',
                                              description   => 'Get a template for the report',
                                              expires       => '-1d',
                                              callback      => sub { $self->_get_report_template( @_ ) } );

    # add the required template_name parameter to the get_report_template() method
    $method->add_input_parameter( name          => 'template_name',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => '' );

    # add the required container_id parameter to the get_report_template() method
    $method->add_input_parameter( name          => 'container_id',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => '' );

    # register the get_report_template() method
    $self->websvc()->register_method( $method );

}

# callbacks

sub _get_reports {

    my ( $self, $method, $args ) = @_;

    my $results = $self->report_ds()->get_reports( $self->process_args( $args ));

    # handle error
    if ( !$results ) {

        $method->set_error( $self->report_ds()->error() );
        return;
    }

    return $results; 
}

sub _get_report_details {

    my ( $self, $method, $args ) = @_;

    my $results = $self->report_ds()->get_report_details( $self->process_args( $args ));

    # handle error
    if ( !$results ) {

        $method->set_error( $self->report_ds()->error() );
        return;
    }

    return {
        results => $results,
    }
}

sub _get_report_template  {

    my ( $self, $method, $args ) = @_;

    my $results = $self->report_ds()->get_report_template( $self->process_args( $args ));

    # handle error
    if ( !$results ) {

        $method->set_error( $self->report_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub update_constraints_file {

    my ( $self, $constraints_file ) = @_;

    $self->report_ds->update_constraints_file($constraints_file);

}

1;

