#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
#--------------------------------------------------------------------
#----- GRNOC TSDS Library
#-----
#----- Copyright(C) 2015 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS.pm $
#----- $Id: TSDS.pm 39930 2015-10-29 12:35:47Z mrmccrac $
#-----
#----- This module doesn't do much other than storing the version
#----- of TSDS.  All the magic happens in the DataService and GWS
#----- libraries.
#--------------------------------------------------------------------

package GRNOC::TSDS;

use strict;
use warnings;

our $VERSION = '1.8.5';

sub new {
    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    my $self = {
        @_
    };

    bless( $self, $class );

    return $self;
}

sub get_version {
    my $self = shift;

    return $VERSION;
}

1;
