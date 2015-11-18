package GRNOC::TSDS::Writer::EventMessage;

use Moo;

use Types::Standard qw( Str Int HashRef Object Maybe );
use Type::XSD::Lite qw( NonNegativeInteger );

use GRNOC::TSDS::Event;

use Hash::Merge qw( merge );
use Data::Dumper;

### required attributes ###

has 'data_type' => ( is => 'ro',
                     isa => Object,
                     required => 1 );

has 'affected' => ( is => 'ro',
                    isa => HashRef,
                    required => 1,
                    coerce => sub { _coerce_affected( @_ ) } );

has 'text' => ( is => 'ro',
                isa => Str,
                required => 1 );

has 'start' => ( is => 'ro',
                 isa => NonNegativeInteger,
                 required => 1 );

has 'end' => ( is => 'ro',
               isa => Maybe[NonNegativeInteger],
               required => 1 );

has 'identifier' => ( is => 'ro',
                      isa => Str,
                      required => 1 );

has 'type' => ( is => 'ro',
                isa => Str,
                required => 1 );

### lazy attributes ###

has 'event' => ( is => 'lazy' );

### constructor builder ###

sub BUILD {

    my ( $self ) = @_;

    my @affected_fields = keys( %{$self->affected} );
    my $meta_fields = $self->data_type->metadata_fields;
    my $data_type_name = $self->data_type->name;

    # make sure all given affected fields are known meta fields
    foreach my $affected_field ( @affected_fields ) {

        if ( !$meta_fields->{$affected_field} ) {

            die( "Unknown meta field $affected_field for data type $data_type_name." );
        }
    }
}

### private methods ###

sub _build_event {

    my ( $self ) = @_;

    my $event = GRNOC::TSDS::Event->new( data_type => $self->data_type,
                                         type => $self->type,
                                         start => $self->start,
                                         end => $self->end,
                                         identifier => $self->identifier,
                                         affected => $self->affected,
                                         text => $self->text );

    return $event;
}

sub _coerce_affected {

    my ( $affected ) = @_;

    my $results = {};

    while ( my ( $key, $value ) = each( %$affected ) ) {

        my @pieces = split( /\./, $key );

        my $result = _parse_affected_pieces( $value, \@pieces );

        # merge each affected result into final results
        $results = merge( $results, $result );
    }

    return $results;
}

sub _parse_affected_pieces {

    my ( $value, $pieces ) = @_;

    # scalar base case
    return $value if ( @$pieces == 0 );

    my $piece = shift( @$pieces );
    my $next = _parse_affected_pieces( $value, $pieces );

    return {$piece => $next};
}

1;
