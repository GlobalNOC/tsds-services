#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::Upgrade::1_2_1;

use strict;
use warnings;

use constant PREVIOUS_VERSION => '1.2.0';

sub upgrade {

    my ( $self, $upgrade ) = @_;

    ### UPGRADE CODE GOES HERE ###

    ### END UPGRADE CODE ###

    return 1;
}

1;
