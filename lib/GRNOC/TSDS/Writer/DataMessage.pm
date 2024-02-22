#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::Writer::DataMessage;

use Moo;

use Types::Standard qw( Str Int HashRef Object );
use Types::XSD::Lite qw( PositiveInteger NonNegativeInteger );

use GRNOC::TSDS::DataPoint;

use Data::Dumper;

### required attributes ###

has 'data_type' => ( is => 'ro',
                     isa => Object,
                     required => 1 );

has 'time' => ( is => 'ro',
                isa => NonNegativeInteger,
                required => 1 );

has 'interval' => ( is => 'ro',
                    isa => PositiveInteger,
                    required => 1 );

has 'values' => ( is => 'ro',
                  isa => HashRef,
                  required => 1 );

has 'meta' => ( is => 'ro',
                isa => HashRef,
                required => 1 );

### lazy attributes ###

has 'data_points' => ( is => 'lazy' );

has 'measurement_identifier' => ( is => 'lazy' );

### constructor builder ###

sub BUILD {

    my ( $self ) = @_;

    # make sure all required meta fields for this data type were given
    $self->_validate_required_meta_fields();

    return $self;
}

### attribute builders ###

sub _build_data_points {

    my ( $self ) = @_;

    my $data_type = $self->data_type;
    my $time = $self->time;
    my $values = $self->values;

    my $data_points = [];

    while ( my ( $value_type, $value ) = each( %$values ) ) {

        my $data_point = GRNOC::TSDS::DataPoint->new( data_type => $data_type,
                                                      value_type => $value_type,
                                                      time => $time,
                                                      value => $value,
                                                      interval => $self->interval );

        push( @$data_points, $data_point );
    }

    return $data_points;
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
