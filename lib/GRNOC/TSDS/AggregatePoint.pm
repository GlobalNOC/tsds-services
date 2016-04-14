package GRNOC::TSDS::AggregatePoint;

use Moo;
use Types::Standard qw( Str Int StrictNum InstanceOf Maybe Dict Map Num);
use Types::Common::Numeric qw( PositiveInt PositiveOrZeroInt PositiveNum );

use Data::Dumper;

### required attributes ###

has 'data_type' => ( is => 'ro',
                     isa => InstanceOf['GRNOC::TSDS::DataType'],
                     required => 1 );

has 'value_type' => ( is => 'ro',
                      isa => Str,
                      required => 1 );

has 'time' => ( is => 'ro',
                isa => Int,
                required => 1,
                coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

has 'interval' => ( is => 'rw',
                    isa => Int,
                    required => 1,
                    coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

has 'value' => ( is => 'ro',
                 isa => Dict[ 'avg' => Maybe[StrictNum],
                              'min' => Maybe[StrictNum],
                              'max' => Maybe[StrictNum],
                              'hist' => Maybe[ Dict[ 'num_bins' => PositiveInt,
                                                     'bin_size' => PositiveNum,
                                                     'min' => Num,
                                                     'max' => Num,
                                                     'bins' => Map[ Int, Int ],
                                                     'total' => Int ] ] ],
                 required => 1 );

1;
