#!/usr/bin/perl

use strict;
use warnings;

use JSON::XS;
use Net::AMQP::RabbitMQ;

use Getopt::Long;
use Data::Dumper;

### constants ###

use constant TYPE => 'interface';
use constant INTERVAL => 30;
use constant MESSAGE_SIZE => 100;

### command line options ###

# 30 seconds * 86,400 = 1 month of data by default
my $num_updates = 86_400;
my $interval = 30;
my $num_measurements = 10;
my $rabbit_host = '127.0.0.1';
my $rabbit_port = 5672;
my $rabbit_queue = 'timeseries_data';
my $mode = 'sin-cos';
my $start = 0;
my $help;

GetOptions( 'num-updates=i' => \$num_updates,
            'num-measurements=i' => \$num_measurements,
            'rabbit-host=s' => \$rabbit_host,
            'rabbit-port=i' => \$rabbit_port,
            'rabbit-queue=s' => \$rabbit_queue,
            'start=i' => \$start,
            'interval=i' => \$interval,
            'mode=s' => \$mode,
            'help|h|?' => \$help );

usage() if $help;

my $rabbit = Net::AMQP::RabbitMQ->new();

$rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );
$rabbit->channel_open( 1 );

my $timestamp = $start;
my $counter = 0;
my $messages = [];

while ( 1 ) {

    last if ( $counter == $num_updates );

    my $data = make_data( time => $timestamp,
                          counter => $counter );

    push( @$messages, @$data );

    if ( @$messages >= MESSAGE_SIZE ) {

        $rabbit->publish( 1, $rabbit_queue, encode_json( $messages ), {'exchange' => ''} );
        $messages = [];
    }

    $counter++;
    $timestamp += $interval;
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

sub usage {

    print "$0 [--rabbit-host <host>] [--rabbit-port <port>] [--rabbit-queue <queue>] [--mode <sin-cos|worst-case|best-case>] [--interval <seconds>] [--start <epoch>] [--num-updates <num>] [--num-measurements <num>] [--help]\n";
    exit ( 1 );
}
