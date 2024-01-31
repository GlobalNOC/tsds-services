package GRNOC::TSDS::DataPoint;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';

use Moo;
use Types::Standard qw( Str Int StrictNum Object Maybe );

use Data::Dumper;

### required attributes ###

has 'data_type' => ( is => 'ro',
                     isa => Object,
                     required => 1 );

has 'value_type' => ( is => 'ro',
                      isa => Str,
                      required => 1 );

has 'time' => ( is => 'ro',
                isa => Int,
                required => 1,
                coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

has 'value' => ( is => 'ro',
                 isa => Maybe[StrictNum],
                 required => 1,
                 coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

### optional attributes ###

has 'interval' => ( is => 'rw',
                    isa => Int,
                    required => 0,
                    coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

1;
