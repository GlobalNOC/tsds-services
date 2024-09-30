#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
#--------------------------------------------------------------------
#----- GRNOC TSDS Query DataService Library
#-----
#----- Copyright(C) 2013 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $LastChangedBy: $
#----- $LastChangedRevision: $
#----- $LastChangedDate: $
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/DataService/Query.pm $
#----- $Id: Query.pm 35129 2015-02-02 20:14:58Z bgeels $
#-----
#----- This module inherits the base GRNOC::TSDS::DataService class
#----- and provides all of the methods to interact with Parser
#--------------------------------------------------------------------

package GRNOC::TSDS::DataService::Query;

use strict;
use warnings;

use base 'GRNOC::TSDS::DataService';

use GRNOC::TSDS::Parser;

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

    # get/store all of the data services we need
    $self->parser( GRNOC::TSDS::Parser->new( @_ ) );

    return $self;
}

sub total {
    my $self = shift;
    return $self->parser()->total();
}

sub total_raw {
    my $self = shift;
    return $self->parser()->total_raw();
}

sub actual_start {
    my $self = shift;
    return $self->parser()->actual_start();
}

sub actual_end {
    my $self = shift;
    return $self->parser()->actual_end();
}

sub run_query {

    my ( $self, %args ) = @_;

    my $query = $args{'query'};

    my $results = $self->parser()->evaluate($query, force_constraint => 1);

    if ( !$results ) {
        $self->error( $self->parser()->error() );
        return;
    }

    return $results;
}

1;

