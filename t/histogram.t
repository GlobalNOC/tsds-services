use strict;
use warnings;

use Test::More tests => 45;

use GRNOC::TSDS::Aggregate::Histogram;
use Data::Dumper;

# first, create a histogram without specifying a min and max for range
my $hist = GRNOC::TSDS::Aggregate::Histogram->new( hist_min => undef,
                                                   hist_max => undef,
                                                   data_min => 0,
                                                   data_max => 10,
                                                   min_width => 0.001,
                                                   resolution => 0.1 );


# verify initial values of histogram
is( $hist->data_min(), 0, 'data_min' );
is( $hist->data_max(), 10, 'data_max' );
is( $hist->resolution(), 0.1, 'resolution' );
is( $hist->num_bins(), 1000, 'num_bins' );
is( $hist->bin_size(), 0.01, 'bin_size' );
is( $hist->total(), 0, 'total' );

# add some example values to the histogram
$hist->add_values( [0, 0.001, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9.999, 10] );

is( $hist->total(), 13, "new total" );

my $bins = $hist->bins();

is( $bins->{0}, 2, "2 items in bin 0" );
is( $bins->{100}, 1, "1 item in bin 100" );
is( $bins->{200}, 1, "1 item in bin 200" );
is( $bins->{300}, 1, "1 item in bin 300" );
is( $bins->{400}, 1, "1 item in bin 400" );
is( $bins->{500}, 1, "1 item in bin 500" );
is( $bins->{600}, 1, "1 item in bin 600" );
is( $bins->{700}, 1, "1 item in bin 700" );
is( $bins->{800}, 1, "1 item in bin 800" );
is( $bins->{900}, 1, "1 item in bin 900" );
is( $bins->{999}, 2, "2 items in bin 999" );

is( $hist->get_index( 55 ), 5499, 'index' );

# now re-do the histogram but provided a min and max range for it
$hist = GRNOC::TSDS::Aggregate::Histogram->new( hist_min => 0,
                                                hist_max => 10,
                                                data_min => 0,
                                                data_max => 10,
                                                min_width => 0.001,
                                                resolution => 0.1 );

# add some example values to the histogram
$hist->add_values( [0, 0.001, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9.999, 10] );

$bins = $hist->bins();

is( $bins->{0}, 2, "2 items in bin 0" );
is( $bins->{100}, 1, "1 item in bin 100" );
is( $bins->{200}, 1, "1 item in bin 200" );
is( $bins->{300}, 1, "1 item in bin 300" );
is( $bins->{400}, 1, "1 item in bin 400" );
is( $bins->{500}, 1, "1 item in bin 500" );
is( $bins->{600}, 1, "1 item in bin 600" );
is( $bins->{700}, 1, "1 item in bin 700" );
is( $bins->{800}, 1, "1 item in bin 800" );
is( $bins->{900}, 1, "1 item in bin 900" );
is( $bins->{999}, 2, "2 items in bin 999" );

is( $hist->get_index( 55 ), 5499, 'index' );

# re-do it again and supply a different min and max range
$hist = GRNOC::TSDS::Aggregate::Histogram->new( hist_min => 0,
                                                hist_max => 100,
                                                data_min => 0,
                                                data_max => 100,
                                                min_width => 0.001,
                                                resolution => 0.1 );

# add some example values to the histogram
$hist->add_values( [0, 0.001, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9.999, 10, 99.999, 100] );

$bins = $hist->bins();

is( $bins->{0}, 2, "2 items in bin 0" );
is( $bins->{10}, 1, "1 item in bin 10" );
is( $bins->{20}, 1, "1 item in bin 20" );
is( $bins->{30}, 1, "1 item in bin 30" );
is( $bins->{40}, 1, "1 item in bin 40" );
is( $bins->{50}, 1, "1 item in bin 50" );
is( $bins->{60}, 1, "1 item in bin 60" );
is( $bins->{70}, 1, "1 item in bin 70" );
is( $bins->{80}, 1, "1 item in bin 80" );
is( $bins->{90}, 1, "1 item in bin 90" );
is( $bins->{99}, 1, "1 items in bin 99" );
is( $bins->{100}, 1, "1 items in bin 100" );
is( $bins->{999}, 2, "2 items in bin 999" );

is( $hist->get_index( 55 ), 550, 'index' );
