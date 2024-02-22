#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::Upgrade::1_4_0;

use strict;
use warnings;

use GRNOC::TSDS::Install;
use GRNOC::TSDS::MongoDB;
use Tie::IxHash;
use Data::Dumper;

use constant PREVIOUS_VERSION => '1.2.3';

sub upgrade {

    my ( $self, $upgrade ) = @_;

    ### UPGRADE CODE GOES HERE ###

    ### END UPGRADE CODE ###

    return 1;
}

1;
