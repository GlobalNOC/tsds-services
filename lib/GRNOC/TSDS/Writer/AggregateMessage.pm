#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::Writer::AggregateMessage;

use Moo;

use Types::Standard qw( HashRef InstanceOf );
use Types::Common::Numeric qw( PositiveInt PositiveOrZeroInt );

use Data::Dumper;

### required attributes ###

has 'data_type' => ( is => 'ro',
                     isa => InstanceOf['GRNOC::TSDS::DataType'],
                     required => 1 );

has 'time' => ( is => 'ro',
                isa => PositiveOrZeroInt,
                required => 1 );

has 'interval' => ( is => 'ro',
                    isa => PositiveInt,
                    required => 1 );

has 'values' => ( is => 'ro',
                  isa => HashRef,
                  required => 1 );

has 'meta' => ( is => 'ro',
                isa => HashRef,
                required => 1 );

### lazy attributes ###

has 'aggregate_points' => ( is => 'lazy' );

has 'measurement_identifier' => ( is => 'lazy' );

### constructor builder ###

sub BUILD {

    my ( $self ) = @_;

    # make sure all required meta fields for this data type were given
    $self->_validate_required_meta_fields();

    return $self;
}

### attribute builders ###

sub _build_aggregate_points {

    my ( $self ) = @_;

    my $data_type = $self->data_type;
    my $time = $self->time;
    my $values = $self->values;

    my $aggregate_points = [];

    while ( my ( $value_type, $value ) = each( %$values ) ) {

        my $aggregate_point = GRNOC::TSDS::AggregatePoint->new( data_type => $data_type,
                                                                value_type => $value_type,
                                                                time => $time,
                                                                value => $value,
                                                                interval => $self->interval );

        push( @$aggregate_points, $aggregate_point );
    }

    return $aggregate_points;
}

sub _build_measurement_identifier {

    my ( $self ) = @_;

    my $hash = Digest::SHA->new( 256 );

    # grab the required metadata fields from the data type
    my $metadata_fields = $self->data_type->metadata_fields;

    # important to sort the fields in order so the hash is always the same
    my @fields = sort( keys( %$metadata_fields ) );

    foreach my $field ( @fields ) {

        # skip it if its not a required field
        next if ( !$metadata_fields->{$field}{'required'} );

        # include this field value in our hash
        $hash->add( $self->meta->{$field} );
    }

    # return the final digest of the hash
    return $hash->hexdigest();
}

### private methods ###

sub _validate_required_meta_fields {

    my ( $self ) = @_;

    my $data_type = $self->data_type;

    my $data_type_name = $data_type->name;
    my $data_type_metadata_fields = $data_type->metadata_fields;

    # make sure all required meta fields for this data type were specified
    my @field_names = keys( %$data_type_metadata_fields );

    foreach my $field_name ( @field_names ) {

        # this metadata field is not required, skip it
        next if ( !$data_type_metadata_fields->{$field_name}{'required'} );

        # make sure this required meta field was provided
        if ( !defined( $self->meta->{$field_name} ) ) {

            die( "Required meta field $field_name not specified for data type $data_type_name." );
        }
    }
}

1;
