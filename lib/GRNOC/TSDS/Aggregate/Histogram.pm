#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::Aggregate::Histogram;

use strict;
use warnings;

use Math::Round qw( nlowmult nhimult );

### constants ###

use constant DEFAULT_RESOLUTION => 1.0;
use constant BIN_MULTIPLE => 10;
use constant MIN_BIN_SIZE => 0.00001;

### constructor ###

sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    my $self = {'hist_min' => undef,
                'hist_max' => undef,
                'data_min' => undef,
                'data_max' => undef,
                'min_width' => undef,
                'resolution' => DEFAULT_RESOLUTION,
                'bin_size' => undef,
                'bins' => {},
                'num_bins' => 0,
                'total' => 0,
                @_};

    bless( $self, $class );

    # data_min & data_max both required
    return if ( !defined( $self->data_min ) && !defined( $self->data_max ) );

    # dont allow histograms where max == min
    return if ( $self->data_min == $self->data_max );

    # do we need to determine the proper bin size / min / max?
    $self->_init_attribs();

    # were we unable to determine an appropriate bin size?
    return if ( !$self->bin_size );

    # set the total number of bins will be with this bin size, min, and max
    $self->num_bins( int( ( $self->hist_max - $self->hist_min ) / $self->bin_size ) );

    return $self;
}

### getters/setters ###

sub bins {

    my ( $self, $bins ) = @_;

    # did they supply a new set of bins to us?
    if ( defined( $bins ) ) {

        $self->{'bins'} = $bins;

        # determine the new count total across all bins in this histogram
        my @counts = values( %{$self->{'bins'}} );

        my $new_total = 0;

        foreach my $count ( @counts ) {

            $new_total += $count;
        }

        $self->{'total'} = $new_total;
    }

    # Make sure we get it in numerical context, yay perl
    return $self->{'bins'};
}

sub bin_size {

    my ( $self, $bin_size ) = @_;

    $self->{'bin_size'} = $bin_size if ( defined( $bin_size ) );

    # Make sure we get it in numerical context, yay perl
    return defined($self->{'bin_size'}) ? $self->{'bin_size'} + 0 : $self->{'bin_size'};
}

sub min_width {

    my ( $self, $min_width ) = @_;

    $self->{'min_width'} = $min_width if ( defined( $min_width ) );

    # Make sure we get it in numerical context, yay perl
    return defined($self->{'min_width'}) ? $self->{'min_width'} + 0 : $self->{'min_width'};
}

sub num_bins {

    my ( $self, $num_bins ) = @_;

    $self->{'num_bins'} = $num_bins if ( defined( $num_bins ) );

    # Make sure we get it in numerical context, yay perl
    return defined($self->{'num_bins'}) ? $self->{'num_bins'} + 0 : $self->{'num_bins'};
}

sub data_min {

    my ( $self, $data_min ) = @_;

    $self->{'data_min'} = $data_min if ( defined( $data_min ) );

    # Make sure we get it in numerical context, yay perl
    return defined($self->{'data_min'}) ? $self->{'data_min'} + 0 : $self->{'data_min'};
}

sub data_max {

    my ( $self, $data_max ) = @_;

    $self->{'data_max'} = $data_max if ( defined( $data_max ) );

    # Make sure we get it in numerical context, yay perl
    return defined($self->{'data_max'}) ? $self->{'data_max'} + 0 : $self->{'data_max'};
}

sub hist_min {

    my ( $self, $hist_min ) = @_;

    $self->{'hist_min'} = $hist_min if ( defined( $hist_min ) );

    # Make sure we get it in numerical context, yay perl
    return defined($self->{'hist_min'}) ? $self->{'hist_min'} + 0 : $self->{'hist_min'};
}

sub hist_max {

    my ( $self, $hist_max ) = @_;

    $self->{'hist_max'} = $hist_max if ( defined( $hist_max ) );

    # Make sure we get it in numerical context, yay perl
    return defined($self->{'hist_max'}) ? $self->{'hist_max'} + 0 : $self->{'hist_max'};
}

sub resolution {

    my ( $self, $resolution ) = @_;

    $self->{'resolution'} = $resolution if ( defined( $resolution ) );

    # Make sure we get it in numerical context, yay perl
    return defined($self->{'resolution'}) ? $self->{'resolution'} + 0 : $self->{'resolution'};
}

sub total {

    my ( $self, $total ) = @_;

    $self->{'total'} = $total if ( defined( $total ) );

    # Make sure we get it in numerical context, yay perl
    return defined($self->{'total'}) ? $self->{'total'} + 0 : $self->{'total'};
}

### public methods ###

sub add_values {

    my ( $self, $values ) = @_;

    my $bin_size = $self->bin_size();
    my $min = $self->hist_min();
    my $num_bins = $self->num_bins();
    my $bins = $self->bins();

    # do this in C...
    my $total = GRNOC::TSDS::Aggregate::Histogram::_add_values( $bin_size, $min, $num_bins, $bins, $values );

    $self->{'total'} += $total;

    return 1;
}

sub get_midpoint {

    my ( $self, $bin ) = @_;

    my $min = $self->hist_min();
    my $bin_size = $self->bin_size();

    my $offset = $min + ( $bin * $bin_size );
    my $midpoint = $offset + ( $bin_size / 2 );

    return $midpoint;
}

sub get_index {

    my ( $self, $value ) = @_;

    # do this in C...
    return GRNOC::TSDS::Aggregate::Histogram::_get_index( $self->bin_size(), $self->hist_min(), $self->num_bins(), $value );
}

### private methods ###

sub _init_attribs {

    my ( $self ) = @_;

    # did they supply a bin size to us?
    if ( $self->bin_size ) {

        my $min = nlowmult( $self->bin_size, $self->data_min );
        my $max = nhimult( $self->bin_size, $self->data_max );

        $self->hist_min( $min );
        $self->hist_max( $max );

        return;
    }

    my $min = $self->hist_min;
    my $max = $self->hist_max;

    $min = $self->data_min if ( !defined( $min ) );
    $max = $self->data_max if ( !defined( $max ) );

    # store the attributes of the best bin size as we find one
    my $best_bin_size;
    my $best_num_bins;
    my $best_start_bin;
    my $best_end_bin;

    my $range = $max - $min;

    # determine desired number of bins based upon desired resolution
    my $desired_num_bins = 100 / $self->resolution();

    # If we have say 600Gbps, we want to use a power of 10 multipled by 2
    # to maintain a consistent 10 aligned boundary but scaled up accordingly, while also ensuring
    # that histograms generated from this will be combinable (all powers of 10 and multiples of 2)
    my $mult = 1;
    if ($range >= 1){
	$mult = int($range / (10 ** int(log($range) / log(10))));
	$mult = 2 if ($mult > 1);
	$mult = 1 if ($mult < 1);
    }
    

    # find the best bin size
    for ( my $bin_size = MIN_BIN_SIZE * $mult; $bin_size < $range; $bin_size *= BIN_MULTIPLE ) {

        # round the bins based upon our bin size
        my $start_bin = nlowmult( $bin_size, $min );
        my $end_bin = nhimult( $bin_size, $max );

        # determine what the total number of bins will be with this bin size
        my $num_bins = ( $end_bin - $start_bin ) / $bin_size;

        # this and larger bin sizes won't give us our necessary accuracy
        last if ( $num_bins < $desired_num_bins );

        # this is the current largest bin size which still gives us the necessary accuracy
        $best_bin_size = $bin_size;
        $best_num_bins = $num_bins;
        $best_start_bin = $start_bin;
        $best_end_bin = $end_bin;
    }

    # too small bin size
    if ( !$best_bin_size || $best_bin_size < $self->min_width ) {

        $best_bin_size = $self->min_width;
        $best_start_bin = nlowmult( $best_bin_size, $self->data_min );
        $best_end_bin = nhimult( $best_bin_size, $self->data_max );
    }

    # store all of the calculated attributes of this histogram based upon min, max, and resolution
    $self->bin_size( $best_bin_size );
    $self->hist_min( $best_start_bin );
    $self->hist_max( $best_end_bin );

    return;
}

1;