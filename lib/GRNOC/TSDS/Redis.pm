package GRNOC::TSDS::Redis;

use strict;
use warnings;

use Data::Dumper;
use Redis;

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

    my $conn_str = $self->{'config'}->redis_host . ':' . $self->{'config'}->redis_port;
    log_debug("Connecting to Redis: $conn_str");
    $self->{'redis'} = new Redis(
        server => $conn_str,
        reconnect => 120,
        every => 3 * 1000 * 1000, # microseconds
    );
    return $self;
}

sub redis {
    my $self = shift;
    return $self->{'redis'};
}

1;
