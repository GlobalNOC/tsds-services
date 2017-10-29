package GRNOC::TSDS::Upgrade::1_5_4;

use strict;
use warnings;

use GRNOC::TSDS::Install;
use GRNOC::TSDS::MongoDB;
use Tie::IxHash;
use Data::Dumper;

use constant PREVIOUS_VERSION => '1.5.3';

sub upgrade {

    my ( $self, $upgrade ) = @_;

    ### UPGRADE CODE GOES HERE ###

    # NOTHING HERE

    my $mongo = $upgrade->mongo_root;

    return 1;
}

1;
