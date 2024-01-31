#--------------------------------------------------------------------
#----- GRNOC TSDS Search GWS Library
#-----
#----- Copyright(C) 2013 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $LastChangedBy: $
#----- $LastChangedRevision: $
#----- $LastChangedDate: $
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/GWS/Search.pm $
#----- $Id: Search.pm 35847 2015-03-10 15:43:14Z mj82$
#-----
#----- This module inherits the base GRNOC::TSDS::GWS class and
#----- provides all of the webservice methods to interact with the
#----- Search DataService.
#--------------------------------------------------------------------

package GRNOC::TSDS::GWS::Search;

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';

use base 'GRNOC::TSDS::GWS';

use GRNOC::TSDS::DataService::Search;

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
    $self->search_ds( GRNOC::TSDS::DataService::Search->new( @_ ) );

    return $self;
}

sub _init_methods {

    my $self = shift;

    my $method;

    $method = GRNOC::WebService::Method->new( name          => 'search',
                                              description   => 'Performs a global search.',
                                              expires       => '-1d',
                                              callback      => sub { $self->_search( @_ ) } );

    $method->add_input_parameter( name          => 'search',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Search term to use for the global search.' );

    $method->add_input_parameter( name          => 'limit',
                                  pattern       => $NUMBER_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Allows you to set the result limit.' );

    $method->add_input_parameter( name          => 'offset',
                                  pattern       => $NUMBER_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Allows you to set the result offset.' );
    
    $method->add_input_parameter( name          => 'step',
                                  pattern       => $NUMBER_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Allows you to set the time (in seconds) the sparkline points are aggregated by.' );

    $method->add_input_parameter( name          => 'start_time',
                                  pattern       => $NUMBER_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Accepts the start time as a unix epoch.' );
    
    $method->add_input_parameter( name          => 'end_time',
                                  pattern       => $NUMBER_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Accepts the end time as a unix epoch, defaults to now if nothing is passed in' );
    
    $method->add_input_parameter( name          => 'measurement_type',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 1,
                                  description   => 'Optional list of masurement types to search (searches all by default)' );
    
    $method->add_input_parameter( name          => 'group_by',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 1,
                                  description   => 'Allows you to control which meta fields the results will be grouped by. (Only available when constrained to one measurement_type)' );
    
    $method->add_input_parameter( name          => 'order_by',
                                  pattern       => '^((name|value)_\d+)$',
                                  required      => 0,
                                  multiple      => 1,
                                  description   => 'Optional list of fields to order by' );
    
    $method->add_input_parameter( name          => 'order',
                                  pattern       => '^(asc|desc)$',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Order of the fields you are ordering by' );
    
    $method->add_input_parameter( name          => 'meta_field_name',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 1,
                                  description   => 'Defines a meta field to filter on. Must have corresponding meta_field_value and meta_field_logic parameters.' );
    
    $method->add_input_parameter( name          => 'meta_field_value',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 1,
                                  description   => 'Defines the value of a meta field to filter on. Must have corresponding meta_field_name and meta_field_logic parameters' );
    
    $method->add_input_parameter( name          => 'meta_field_logic',
                                  pattern       => '^(is|is_not|contains|does_not_contain)$',
                                  required      => 0,
                                  multiple      => 1,
                                  description   => 'Defines the logic of a meta field to filter on. Must have corresponding meta_field_name and meta_field_value parameters' );

    $method->add_input_parameter( name          => 'value_field_name',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 1,
                                  description   => 'Defines a value field to filter on. Must have corresponding value_field_value, value_field_logic, and value_field_function parameters.' );
    
    $method->add_input_parameter( name          => 'value_field_value',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 1,
                                  description   => 'Defines the value of a value field to filter on. Must have corresponding value_field_name, value_field_logic, and value_field_function parameters' );
    
    $method->add_input_parameter( name          => 'value_field_logic',
                                  pattern       => '^(<|<=|=|!=|>=|>)$',
                                  required      => 0,
                                  multiple      => 1,
                                  description   => 'Defines the logic of a value field to filter on. Must have corresponding value_field_name, value_field_value, and value_field_function parameters' );

    $method->add_input_parameter( name          => 'value_field_function',
                                  pattern       => '^(min|max|average|percentile_95)$',
                                  required      => 0,
                                  multiple      => 1,
                                  description   => 'Defines the aggregation function of a value field to filter on. Must have corresponding value_field_name, value_field_value, and value_field_logic parameters' );

    # register the search() method
    $self->websvc()->register_method( $method );

}

# callbacks

sub _search {

    my ( $self, $method, $args ) = @_;

    my $ret = $self->search_ds()->search( $self->process_args( $args ) );

    # handle error
    if ( !$ret ) {

        $method->set_error( $self->search_ds()->error() );
        return;
    }

    return $ret;
}

sub update_constraints_file {

    my ( $self, $constraints_file ) = @_;

    $self->search_ds->update_constraints_file($constraints_file);

}

1;

