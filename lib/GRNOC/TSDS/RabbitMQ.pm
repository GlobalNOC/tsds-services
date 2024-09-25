package GRNOC::TSDS::RabbitMQ;

use strict;
use warnings;

use Net::AMQP::RabbitMQ;
use Data::Dumper;

use GRNOC::Log;
use GRNOC::TSDS::Config;


use constant DATA_CACHE_EXPIRATION => 60 * 60;
use constant AGGREGATE_CACHE_EXPIRATION => 60 * 60 * 48;
use constant MEASUREMENT_CACHE_EXPIRATION => 60 * 60;
use constant QUEUE_PREFETCH_COUNT => 5;
use constant QUEUE_FETCH_TIMEOUT => 10 * 1000;
use constant RECONNECT_TIMEOUT => 10;
use constant PENDING_QUEUE_CHANNEL => 1;
use constant FAILED_QUEUE_CHANNEL => 2;


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

    my $conn_str = $self->{'config'}->rabbitmq_host . ':' . $self->{'config'}->rabbitmq_port;
    log_debug("Connecting to RabbitMQ: $conn_str");
    
    $self->{'rabbitmq'} = new Net::AMQP::RabbitMQ();
    $self->{'rabbitmq'}->connect(
        $self->{'config'}->rabbitmq_host,
        {port => $self->{'config'}->rabbitmq_port}
    );

    return $self;
}

sub rabbitmq {
    my $self = shift;
    return $self->{'rabbitmq'};
}

1;
