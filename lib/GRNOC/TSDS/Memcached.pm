package GRNOC::TSDS::Memcached;

use strict;
use warnings;

use Cache::Memcached::Fast;
use Data::Dumper;

use GRNOC::Log;
use GRNOC::TSDS::Config;


=head2 new

=cut
sub new {
    my $class = shift;
    my $args  = {
        config => undef, # GRNOC::TSDS::Config
        @_
    };
    my $self = bless $args, $class;

    $self->{'config'} = $args->{'config'};

    my $conn_str = $self->{'config'}->memcached_host . ':' . $self->{'config'}->memcached_port;
    log_debug("Connecting to Memcached: $conn_str");
    $self->{'memcached'} = new Cache::Memcached::Fast({
        'servers' => [
            {'address' => $conn_str, 'weight' => 1}
        ]
    });
    return $self;
}

sub memcached {
    my $self = shift;
    return $self->{'memcached'};
}

1;
