#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
#--------------------------------------------------------------------
#----- GRNOC TSDS Push GWS Library
#-----
#----- Copyright(C) 2013 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $LastChangedBy: $
#----- $LastChangedRevision: $
#----- $LastChangedDate: $
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/GWS/Push.pm $
#----- $Id: Push.pm 31432 2014-06-16 21:02:01Z charmadu $
#-----
#----- This module inherits the base GRNOC::TSDS::GWS class and
#----- provides all of the webservice methods to interact with the
#----- Push DataService.
#--------------------------------------------------------------------

package GRNOC::TSDS::GWS::Push;

use strict;
use warnings;

use base 'GRNOC::TSDS::GWS';

use GRNOC::TSDS::DataService::Push;
use GRNOC::TSDS::InfluxDB;

use GRNOC::WebService::Method;
use GRNOC::WebService::Regex;

use Data::Dumper;
use JSON;

sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    my $self = $class->SUPER::new( @_ );

    bless( $self, $class );

    # get/store our data service
    $self->push_ds( GRNOC::TSDS::DataService::Push->new( @_ ) );
    $self->influxdb( GRNOC::TSDS::InfluxDB->new( @_) );

    return $self;
}

sub _init_methods {

    my $self = shift;

    my $method;

    $method = GRNOC::WebService::Method->new( name          => 'add_data',
                                              description   => 'Add data to Rabbit',
                                              expires       => '-1d',
                                              callback      => sub { $self->_add_data( @_ ) } );

    # add the required data parameter to the add_data() method
    $method->add_input_parameter( name          => 'data',
                                  pattern       => $TEXT,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => '' );
    $self->websvc()->register_method( $method );

    my $add_influx_data = GRNOC::WebService::Method->new(
        name          => 'add_influx_data',
        description   => 'Add data to Rabbit with InfluxDB Line Protocol',
        expires       => '-1d',
        callback      => sub { $self->_add_influx_data( @_ ) },
    );
    $add_influx_data->add_input_parameter(
        name          => 'data',
        pattern       => $TEXT,
        required      => 1,
        multiple      => 0,
        description   => '',
    );
    $self->websvc()->register_method($add_influx_data);
}

# callbacks

sub _add_data {

    my ( $self, $method, $args ) = @_;

    my %processed = $self->process_args( $args );
    $processed{'user'} = $ENV{'REMOTE_USER'};

    my $results = $self->push_ds()->add_data( %processed );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->push_ds()->error() );
        return;
    }

    return {
	    results => $results,
    };
}


sub _add_influx_data {
    my ( $self, $method, $args ) = @_;

    my %processed = $self->process_args( $args );

    $processed{'user'} = $ENV{'REMOTE_USER'};

    # Convert Line Protocol into traditional TSDS data structures
    eval {
	my $data = $self->influxdb->parse($processed{'data'});
	$processed{'data'} = encode_json($data);
    };
    if ($@) {
	$method->set_error($@);
	return;
    }

    my $results = $self->push_ds()->add_data( %processed );
    if ( !$results ) {
        $method->set_error( $self->push_ds()->error() );
        return;
    }
    return { results => $results };
}

1;

