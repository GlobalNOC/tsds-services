package GRNOC::TSDS::AggregatePoint;

use Moo;
use Types::Standard qw( Str Int StrictNum InstanceOf Maybe );

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

has 'value' => ( is => 'ro',
                 isa => Dict[ 'avg' => Maybe[StrictNum],
			      'min' => Maybe[StrictNum],
			      'max' => Maybe[StrictNum],
			      'hist' => Maybe[ Dict[ 'num_bins' => Int,
						     'bin_size' => Int,
						     'min' => Int,
						     'max' => Int,
						     'bins' => Map[ Int, Int ],
						     'total' => Int ] ] ],						     
		 required => 1 );

### optional attributes ###

has 'interval' => ( is => 'rw',
                    isa => Int,
                    required => 0,
                    coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

1;
