#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5

use strict;
use warnings;

use JSON::XS;
use Net::AMQP::RabbitMQ;
use Net::RabbitMQ::Management::API;

use Getopt::Long;
use Data::Dumper;

### constants ###

use constant TYPE => 'interface';
use constant INTERVAL => 30;
use constant MESSAGE_SIZE => 1000;
use constant QUEUE_CHECK_INTERVAL => 100;
use constant RETRY_TIMEOUT => 10;

### command line options ###

# 30 seconds * 86,400 = 1 month of data by default
my $num_updates = 86_400;
my $interval = 30;
my $num_measurements = 10;
my $rabbit_host = '127.0.0.1';
my $rabbit_port = 5672;
my $rabbit_mgmt_port = 15672;
my $rabbit_queue = 'timeseries_data';
my $mode = 'sin-cos';
my $start = 0;
my $help;
my $queue_size_limit = 0;
my $check_aggregate_queue;
my $aggregate_queue_size_limit = 0;

GetOptions( 'num-updates=i' => \$num_updates,
            'num-measurements=i' => \$num_measurements,
            'rabbit-host=s' => \$rabbit_host,
            'rabbit-port=i' => \$rabbit_port,
            'rabbit-mgmt-port=i' => \$rabbit_mgmt_port,
            'rabbit-queue=s' => \$rabbit_queue,
            'start=i' => \$start,
            'interval=i' => \$interval,
            'mode=s' => \$mode,
            'queue-size-limit=i' => \$queue_size_limit, # 0 means no limit
            'check-aggregate-queue=s' => \$check_aggregate_queue,
            'aggregate-queue-size-limit=i' => \$aggregate_queue_size_limit, # 0 means no limit
            'help|h|?' => \$help );

usage() if $help;

my $rabbit = Net::AMQP::RabbitMQ->new();

$rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );
$rabbit->channel_open( 1 );

my $timestamp = $start;
my $counter = 0;
my $messages = [];
my $num_messages_sent = 0;

while ( 1 ) {

    last if ( $counter == $num_updates );

    my $data = make_data( time => $timestamp,
                          counter => $counter );

    push( @$messages, @$data );

    # Push arrays of size at most MESSAGE_SIZE to RabbitMQ:
    while ( scalar(@$messages) >= MESSAGE_SIZE ) {

        my @chunk = splice @$messages, 0, MESSAGE_SIZE;
        publish( \@chunk );
    }

    $counter++;
    $timestamp += $interval;
}

# Push any remaining messages to RabbitMQ:
if ( scalar(@$messages) > 0) {

    publish( $messages );
}

# all done
exit( 0 );

### helpers ###

sub make_data {

    my ( %args ) = @_;

    my $time = $args{'time'};
    my $counter = $args{'counter'};

    my $input;
    my $output;

    if ( $mode eq 'worst-case' ) {

        $input = rand( 10000000000 );
        $output = rand( 10000000000 );
    }

    elsif ( $mode eq 'best-case' ) {

        $input = 0;
        $output = 0;
    }

    elsif ( $mode eq 'sin-cos' ) {

        $input = sin( $counter / 10 );
        $output = cos( $counter / 10 );
    }

    my $data = [];

    for my $i ( 1 .. $num_measurements ) {

        my $meta = {'network' => 'TestNet',
                    'node' => "rtr$i.indy$i",
                    'intf' => "xe-1/1/$i"};

        push( @$data, {'type' => TYPE,
                       'interval' => $interval + 0,
                       'meta' => $meta,
                       'time' => $time + 0,
                       'values' => {'input' => $input + 0.0,
                                    'output' => $output + 0.0,
                                    'inUcast' => $input + 0.0,
                                    'outUcast' => $output + 0.0,
                                    'inerror' => $input + 0.0,
                                    'outerror' => $output + 0.0,
                                    'indiscard' => $input + 0.0,
                                    'outdiscard' => $output + 0.0,
                                    'status' => 1}} );
    }

    return $data;
}

# Publish a message to RabbitMQ, with rate-limiting
sub publish {

    my $msg = shift;

    check_queue_size() if $num_messages_sent % QUEUE_CHECK_INTERVAL == 0;
    $rabbit->publish( 1, $rabbit_queue, encode_json( $msg ), {'exchange' => ''} );

    $num_messages_sent += 1;
}

# Optionally wait until the data queue and/or an aggregate queue
# have sufficiently-few messages in them
sub check_queue_size {

    my %args = (
        url => "http://$rabbit_host:$rabbit_mgmt_port/api",
    );

    my $rabbit_management = Net::RabbitMQ::Management::API->new( %args );


    while ( 1 ) {

        my %queues;
        $queues{$rabbit_queue} = $queue_size_limit if $queue_size_limit > 0;

        if ( defined($check_aggregate_queue) and $aggregate_queue_size_limit > 0 ) {

            $queues{$check_aggregate_queue} = $aggregate_queue_size_limit;
        }

        my $good;

        foreach my $queue ( keys %queues ) {

            # determine current rabbit queue size
            my $queue_size;

            eval {
                
                $queue_size = $rabbit_management->get_queue( name => $queue,
                                                             vhost => '%2f' )->content()->{'messages'};
            };

            # detect error determining queue size
            if ( $@ ) {
                warn "Error getting queue info: $@";
                $good = 0;
                next;
            }

            my $max = $queues{$queue};

            # we're over the max queue size, sleep awhile before trying again
            if ( $queue_size && $queue_size > $max ) {
                
                warn "[$queue] Queue size is $queue_size > $max, sleeping...";
                $good = 0;
            }

            $good = 1 if (! defined $good);
        }

        if ( ! $good ){
            sleep( RETRY_TIMEOUT );
            next;
        }

        # we're under the max queue size so we're safe to go ahead and publish our message
        last;
    }
}

sub usage {

    print "$0 [--rabbit-host <host>] [--rabbit-port <port>] [--rabbit-queue <queue>] [--mode <sin-cos|worst-case|best-case>] [--interval <seconds>] [--start <epoch>] [--num-updates <num>] [--num-measurements <num>] [--help]\n";
    exit ( 1 );
}
