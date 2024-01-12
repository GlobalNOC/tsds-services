package GRNOC::TSDS::Upgrade::1_6_0;

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';
use lib './venv/lib/perl5';

use GRNOC::TSDS::Install;
use GRNOC::TSDS::MongoDB;
use Tie::IxHash;
use Data::Dumper;

use constant PREVIOUS_VERSION => '1.5.4';

sub upgrade {

    my ( $self, $upgrade ) = @_;

    ### UPGRADE CODE GOES HERE ###

    # NOTHING HERE

    my $mongo = $upgrade->mongo_root;

    return 1;
}

1;
