#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
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

use constant RECONNECT_TIMEOUT => 10;

use Data::Dumper;
use JSON::XS;
use Try::Tiny;

use GRNOC::TSDS::RabbitMQ;


sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    my $self = $class->SUPER::new( @_ );

    bless( $self, $class );

    $self->_setup_push_restrictions();

    return $self;
}

sub _setup_push_restrictions {
    my ( $self ) = @_;

    $self->{'push_restrictions'} = $self->config->tsds_push_users;
}

sub _connect_rabbit {
    my ( $self ) = @_;

    my $connected = 0;
    while (!$connected) {
        try {
            my $rabbitmq_conn = new GRNOC::TSDS::RabbitMQ(config => $self->config);
            $self->{'rabbit'} = $rabbitmq_conn->rabbitmq;

            $self->{'rabbit'}->channel_open(1);
            $connected = 1;
        }
        catch {
            $self->error("Error connecting to RabbitMQ: $_" );
            warn "Reconnecting after " . RECONNECT_TIMEOUT . " seconds...";
            sleep(RECONNECT_TIMEOUT);
        };
    }
    return $connected;
}

sub _validate_message {
    my ( $self, $data, $username) = @_;

    my $restrictions = $self->{'push_restrictions'}{$username};

    return 1 if (! $restrictions);

    my $json;

    # First some basic data structure sanity checking
    eval {
	$json = JSON::XS::decode_json($data);
    };
    if ( $@ ){
	$self->error("Unable to decode data as JSON");
	return;
    }

    if (ref($json) ne 'ARRAY'){
	$self->error("Data must be an array");
	return;
    }

    # Now actual validations
    foreach my $element (@$json){

	# Can the user write to this TSDS type?
	my ($type, $is_metadata) = $element->{'type'} =~ /^(.+?)(\.metadata)?$/;
	if (! exists $restrictions->{$type}){
	    $self->error("User not allowed to send data for type $type");
	    return;
	}

	# Can the user submit updates for metadata patterns
	# matchings its restrictions?
	my $metadata = $element->{'meta'} || {};
	foreach my $key (keys %$metadata){
	    my $value = $metadata->{$key};
	    if (exists $restrictions->{$type}{$key}){
		my $pattern = $restrictions->{$type}{$key};
		if ($value !~ /$pattern/){
		    $self->error("Metadata $key = $value not allowed to be updated for user");
		    return;
		}
	    }
	}
    }

    return 1;
}

sub add_data {

    my ( $self, %args ) = @_;

    my $data     = $args{'data'};
    my $user     = $args{'user'};

    my $rabbit   = $self->{'rabbit'};

    if (! $rabbit || ! $rabbit->is_connected()){
	$self->_connect_rabbit() || return;
	$rabbit = $self->{'rabbit'};
    }

    # If this user is limited to sending data for specific things,
    # we need to unpack the message and make sure it's okay
    $self->_validate_message($data, $user) || return;    

    eval {
        $rabbit->publish( 1, $self->config->rabbitmq_queue, $data, {'exchange' => ''} );
    };

    # detect error
    if ( $@ ) {
        $self->error( 'An error occurred publishing the data: ' . $@);
        return;
    }
     
    return "Data queued to rabbit successfully";
}

1;
