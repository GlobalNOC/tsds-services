#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::InfluxDB;

use GRNOC::Config;
use InfluxDB::LineProtocol  qw(line2data);
use Moo;
use Types::Standard qw( Str Object );
use Data::Dumper;

has config_file => (
    is => 'ro',
    isa => Str
);
has config => (
    is => 'ro',
    isa => Object
);


sub BUILD {
    my ($self, $args) = @_;

    if (defined $args->{config}) {
        $self->{config} = $args->{config};
    } else {
        $self->{config} = new GRNOC::Config(
            config_file => $args->{config_file},
            force_array => 0
        );
    }
}


=head2 parse

Convert Line Protocol (multiple lines) into traditional TSDS data structures

=cut
sub parse {
    my $self = shift;
    my $data = shift;

    my $result = [];
    foreach my $dp (split /\n/, $data) {
        push @$result, $self->parse_line($dp);
    }
    return $result;
}


=head2 parse_line

Convert Line Protocol into traditional TSDS data structures. Timestamp is
divided by 1,000,000.

Input:

    metric,location=eu,server=srv1 value=42 1437072299900001000

Output:

    {
        interval: 60,
        meta: {
            location: "eu",
            server:   "srv1"
        },
        time: 1437072299,
        type: "metric",
        values: {
            value: 42
        }
    }

=cut
sub parse_line {
    my $self = shift;
    my $line = shift;

    # $values and $tags are hashrefs and undef if empty
    my ($measurement, $values, $tags, $timestamp) = line2data($line);

    # Because Line Protocol has no concept of an interval, it's
    # required that `interval` is passed as a `tag` with each
    # datapoint.
    die "interval missing from datapoint" if !exists $tags->{interval};

    return {
        interval => $tags->{interval},
        meta     => defined $tags ? $tags : {},
        time     => $timestamp / 1000000000,
        type     => $measurement,
        values   => defined $values ? $values : {},
    };
}

1;
