#--------------------------------------------------------------------
#----- GRNOC TSDS Query GWS Library
#-----
#----- Copyright(C) 2013 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $LastChangedBy: $
#----- $LastChangedRevision: $
#----- $LastChangedDate: $
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/GWS/Query.pm $
#----- $Id: Query.pm 38349 2015-07-24 15:06:42Z prattadi $
#-----
#----- This module inherits the base GRNOC::TSDS::GWS class and
#----- provides all of the webservice methods to interact with the
#----- Query DataService.
#--------------------------------------------------------------------

package GRNOC::TSDS::GWS::Query;

use strict;
use warnings;

use base 'GRNOC::TSDS::GWS';

use GRNOC::TSDS::DataService::Query;

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
    $self->query_ds( GRNOC::TSDS::DataService::Query->new( @_ ) );

    return $self;
}

sub _init_methods {

    my $self = shift;

    my $method;

    $method = GRNOC::WebService::Method->new( name          => 'query',
                                              description   => 'Executes a query',
                                              expires       => '-1d',
                                              callback      => sub { $self->_query( @_ ) } );

    # add the required query parameter to the query() method
    $method->add_input_parameter( name          => 'query',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => '' );

    # register the query() method
    $self->websvc()->register_method( $method );

}

# callbacks

sub _query {

    my ( $self, $method, $args ) = @_;

    my $results = $self->query_ds()->run_query( $self->process_args( $args ));

    # handle error
    if ( !$results ) {

        $method->set_error( $self->query_ds()->error() );
        return;
    }

    return {
	total     => $self->query_ds()->total(),
	total_raw => $self->query_ds()->total_raw(),
	results   => $results,
	query     => $args->{'query'}{'value'}
    };
}

sub update_constraints_file {

    my ( $self, $constraints_file ) = @_;

    $self->query_ds->update_constraints_file($constraints_file);

}

1;
