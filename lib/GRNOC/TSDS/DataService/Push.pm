#--------------------------------------------------------------------
#----- GRNOC TSDS Push DataService Library
#-----
#----- Copyright(C) 2015 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/DataService/Push.pm $
#----- $Id: Push.pm 35824 2015-03-07 04:43:23Z mrmccrac $
#-----
#----- This module inherits the base GRNOC::TSDS::DataService class
#--------------------------------------------------------------------

package GRNOC::TSDS::DataService::Push;

use strict;
use warnings;

use base 'GRNOC::TSDS::DataService';

use Net::AMQP::RabbitMQ;
use Data::Dumper;

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

    return $self;
}

sub add_data {

    my ( $self, %args ) = @_;

    my $data = $args{'data'};

    my $rabbit_host = $self->config->get( '/config/rabbit/@host' );
    my $rabbit_port = $self->config->get( '/config/rabbit/@port' );
    my $rabbit_queue = $self->config->get( '/config/rabbit/@queue' );
    
    my $rabbit = Net::AMQP::RabbitMQ->new();   

    eval {

	$rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );
	$rabbit->channel_open( 1 );

	$rabbit->publish( 1, $rabbit_queue, $data, {'exchange' => ''} );
    };

    # detect error
    if ( $@ ) {

	$self->error( 'An error occurred publishing the data.' );
	return;
    }
     
    return "data sent successfully";
}

1;
