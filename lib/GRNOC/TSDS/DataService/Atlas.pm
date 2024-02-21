#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
#--------------------------------------------------------------------
#----- GRNOC TSDS Atlas DataService Library
#-----
#----- Copyright(C) 2014 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/DataService/Atlas.pm $
#----- $Id: Atlas.pm 36017 2015-03-20 13:53:55Z daldoyle $
#-----
#----- This module inherits the base GRNOC::TSDS::DataService class.
#-----
#--------------------------------------------------------------------

package GRNOC::TSDS::DataService::Atlas;

use strict;
use warnings;

use base 'GRNOC::TSDS::DataService';

use GRNOC::TSDS::Parser;

use DateTime;
use POSIX qw( strftime );
use XML::Writer;
use GD::Graph::lines;
use GD::Graph::colour qw( add_colour );
use Number::Format qw( format_bytes );
use Data::Dumper;

### constants ###

use constant ONE_MINUTE => 60;
use constant TIME_FRAME => ONE_MINUTE * 5;
use constant ATLAS_DATE_FORMAT => "%a %b %e %H:%M:%S %Z %Y";
use constant TSDS_DATE_FORMAT => "%m/%d/%Y %T UTC";
use constant INPUT_COLOR => '#45d945';
use constant OUTPUT_COLOR => '#0000ff';
use constant TOP_MARGIN => 4;
use constant BOTTOM_MARGIN => 4;
use constant LEFT_MARGIN => 5;
use constant RIGHT_MARGIN => 55;
use constant BPS_PRECISION => 1;
use constant NUM_TIME_TICKS => 6;
use constant NUM_VALUE_TICKS => 16;
use constant SKIP_VALUE_TICKS => 2;
use constant LINE_WIDTH => 2;

### constructor ###

# this will hold the only actual reference to this object
my $singleton;

sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    # if we've created this object (singleton) before, just return it
    return $singleton if ( defined( $singleton ) );

    my $self = $class->SUPER::new( @_ );

    bless( $self, $class );

    # store our newly created object as the singleton
    $singleton = $self;

    # get/store all of the data services we need
    $self->parser( GRNOC::TSDS::Parser->new( @_ ) );

    return $self;
}

### public methods ###

sub get_atlas_data {

    my ( $self, %args ) = @_;

    my $nodes = $args{'nodes'};
    my $interfaces = $args{'interfaces'};

    # method takes a comma delimited list of nodes and interfaces correlated in the same order
    my @nodes = split( /,/, $nodes );
    my @interfaces = split( /,/, $interfaces );

    # human friendly current timestamp of this request
    my $date = POSIX::strftime( ATLAS_DATE_FORMAT, gmtime() );

    # get the most recent data
    my $end = time();
    my $start = $end - TIME_FRAME;

    # convert to TSDS-friendly time strings
    $end = DateTime->from_epoch( epoch => $end );
    $start = DateTime->from_epoch( epoch => $start );

    $end = $end->strftime( TSDS_DATE_FORMAT );
    $start = $start->strftime( TSDS_DATE_FORMAT );

    my $xml;
    my $xml_writer = XML::Writer->new( OUTPUT => \$xml,
                                       DATA_INTENT => 2,
                                       DATA_MODE => 1 );

    $xml_writer->startTag( 'opt', 'timestamp' => $date );

    for ( my $i = 0; $i < @nodes; $i++ ) {

        my $node = $nodes[$i];
        my $interface = $interfaces[$i];

        my $query = "get values.input, values.output, values.inerror, values.outerror, values.inUcast, values.outUcast, values.status between( \"$start\", \"$end\" ) by node, intf from interface where node = \"$node\" and (intf = \"$interface\" or alternate_intf = \"$interface\")";

        my $results = $self->parser()->evaluate( $query, force_constraint => 1 );

        if ( !$results || !$results->[0] ) {

            $self->error( $self->parser()->error() . "\nQuery: $query" );
            return;
        }

        $results = $results->[0];

        # use the most recent known data points
        my $status = $self->_get_most_recent( $results->{'values.status'} );
        my $input = $self->_get_most_recent( $results->{'values.input'} );
        my $output = $self->_get_most_recent( $results->{'values.output'} );
        my $inerror = $self->_get_most_recent( $results->{'values.inerror'} );
        my $outerror = $self->_get_most_recent( $results->{'values.outerror'} );
        my $inpacket = $self->_get_most_recent( $results->{'values.inUcast'} );
        my $outpacket = $self->_get_most_recent( $results->{'values.outUcast'} );

        # begin interface status entry
        $xml_writer->startTag( 'if-stat',
                               'name' => $interface,
                               'node_name' => $node,
                               'stat' => $status );

        # input/output bps
        $xml_writer->startTag( 'bps',
                               'i' => $input,
                               'o' => $output );

        $xml_writer->endTag( 'bps' );

        # input/output pps
        $xml_writer->startTag( 'pps',
                               'i' => $inpacket,
                               'o' => $outpacket );

        $xml_writer->endTag( 'pps' );

        # input/output errors
        $xml_writer->startTag( 'err',
                               'i' => $inerror,
                               'o' => $outerror );

        $xml_writer->endTag( 'err' );

        # all done with this entry
        $xml_writer->endTag( 'if-stat' );
    }

    $xml_writer->endTag( 'opt' );

    return $xml;
}

sub make_atlas_graph {

    my ( $self, %args ) = @_;

    my $node_name = $args{'node_name'};
    my $int_name = $args{'int_name'};
    my $width = $args{'width'};
    my $height = $args{'height'};
    my $mins = $args{'min'};
    my $timezone = $args{'timezone'};
    my $bgcolor = $args{'bgcolor'};

    # get the most recent data
    my $end = time();
    my $duration = $mins * ONE_MINUTE;
    my $start = $end - $duration;

    # convert to TSDS-friendly time strings
    $end = DateTime->from_epoch( epoch => $end );
    $start = DateTime->from_epoch( epoch => $start );

    $end = $end->strftime( TSDS_DATE_FORMAT );
    $start = $start->strftime( TSDS_DATE_FORMAT );

    # determine proper bucket size per pixel
    my $bucket = int( $duration / $width );

    my $query = "get description, aggregate(values.input, $bucket, average) as input, aggregate(values.output, $bucket, average) as output between( \"$start\", \"$end\" ) by node, intf from interface where node = \"$node_name\" and (intf = \"$int_name\" or alternate_intf = \"$int_name\")";

    my $results = $self->parser()->evaluate( $query, force_constraint => 1 );

    if ( !$results || !$results->[0] ) {

        $self->error( $self->parser()->error() . "\nQuery: $query" );
        return;
    }

    $results = $results->[0];

    my $description = $results->{'description'};
    my $input = $results->{'input'};
    my $output = $results->{'output'};

    my @timestamps = map { $_->[0] } @$input;
    my @input_values = map { $_->[1] } @$input;
    my @output_values = map { $_->[1] } @$output;

    my $min_timestamp = $timestamps[0];
    my $max_timestamp = $timestamps[-1];

    my $graph = GD::Graph::lines->new( $width, $height );

    my $color_list = [ add_colour( INPUT_COLOR ), add_colour( OUTPUT_COLOR ) ];

    my $ret = $graph->set( title => "$node_name $int_name - $description",
                           y_label => 'bps',
                           x_min_value => $min_timestamp,
                           x_max_value => $max_timestamp,
                           x_tick_number => NUM_TIME_TICKS,
                           x_number_format => sub { $self->_graph_time_formatter( shift, $timezone ) },
                           y_number_format => sub { $self->_graph_bps_formatter( shift ) },
                           y_tick_number => NUM_VALUE_TICKS,
                           y_min_value => 0,
                           y_label_skip => SKIP_VALUE_TICKS,
                           dclrs => $color_list,
                           line_width => LINE_WIDTH,
                           transparent => 0,
                           bgclr => add_colour( '#' . $bgcolor ),
                           t_margin => TOP_MARGIN,
                           b_margin => BOTTOM_MARGIN,
                           l_margin => LEFT_MARGIN,
                           r_margin => RIGHT_MARGIN,
                           skip_undef => 1 );

    $graph->set_legend( 'Inbound bps', 'Output bps' );

    if ( !$ret ) {

        $self->error( $graph->error() );
    }

    my $gd = $graph->plot( [\@timestamps, \@input_values, \@output_values] );

    if ( !$gd ) {

        $self->error( $graph->error() );
    }

    my $png = $gd->png();

    return $png;
}

### private methods ###

sub _get_most_recent {

    my ( $self, $data ) = @_;

    # examine most recent (last) data first
    for ( my $i = @$data; $i >= 0; $i-- ) {

        my $entry = $data->[$i];

        next if ( !defined( $entry ) );

        my ( $timestamp, $value ) = @$entry;

        next if ( !defined( $value ) );

        # we found the most recent value
        return $value;
    }

    # no defined value found
    return;
}

sub _graph_time_formatter {

    my ( $self, $timestamp, $timezone ) = @_;

    my $dt = DateTime->from_epoch( epoch => $timestamp );

    $dt->set_time_zone( $timezone );

    return $dt->strftime( '%D %T %Z' );
}

sub _graph_bps_formatter {

    my ( $self, $value ) = @_;

    return format_bytes( $value, precision => BPS_PRECISION );
}

1;
