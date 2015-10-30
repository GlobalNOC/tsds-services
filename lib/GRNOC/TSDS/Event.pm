package GRNOC::TSDS::Event;

use Moo;
use Types::Standard qw( Str Int HashRef Object Maybe );

use Data::Dumper;

### required attributes ###

has 'data_type' => ( is => 'ro',
                     isa => Object,
                     required => 1 );

has 'type' => ( is => 'ro',
                isa => Str,
                required => 1 );

has 'start' => ( is => 'ro',
                 isa => Int,
                 required => 1 );

has 'end' => ( is => 'rw',
               isa => Maybe[Int],
               required => 1 );

has 'identifier' => ( is => 'ro',
                      isa => Str,
                      required => 1 );

has 'affected' => ( is => 'rw',
                    isa => HashRef,
                    required => 1 );

has 'text' => ( is => 'ro',
                isa => Str,
                required => 1 );

1;
