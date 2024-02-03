package GRNOC::TSDS::Upgrade::1_2_2;

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';

use constant PREVIOUS_VERSION => '1.2.1';

sub upgrade {

    my ( $self, $upgrade ) = @_;

    ### UPGRADE CODE GOES HERE ###

    ### END UPGRADE CODE ###

    return 1;
}

1;
