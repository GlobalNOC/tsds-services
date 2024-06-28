#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::AggregateDocument;

use Moo;

use GRNOC::TSDS::Constants;
use GRNOC::TSDS::AggregatePoint;

use Tie::IxHash;
use List::Flatten::Recursive;

use Data::Dumper;

### required attributes ###

has 'data_type' => ( is => 'ro',
                     required => 1 );

has 'measurement_identifier' => ( is => 'ro',
                                  required => 1 );

has 'interval' => ( is => 'rw',
                    required => 1,
                    coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

has 'start' => ( is => 'ro',
                 required => 1,
                 coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

has 'end' => ( is => 'ro',
               required => 1,
               coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

### optional attributes ###

has 'aggregate_points' => ( is => 'rw',
                            required => 0,
                            trigger => 1,
                            default => sub { [] } );

has 'value_types' => ( is => 'rw',
                       required => 0,
                       default => sub { {} } );

### internal attributes ###

has 'dimensions' => ( is => 'ro',
                      required => 0,
                      default => sub { [10, 10, 10] } );

### public methods ###

sub add_aggregate_point {

    my ( $self, $aggregate_point ) = @_;

    push( @{$self->aggregate_points}, $aggregate_point );

    # mark value type as being present too
    $self->value_types->{$aggregate_point->value_type} = 1;

    return $self->aggregate_points;
}

sub add_value_types {

    my ( $self, $value_types ) = @_;

    my $interval = $self->interval;

    my $aggregate_collection = $self->data_type->database->get_collection( "data_$interval" );

    #my $updates = {'updated' => time()};
    my $updates = {'updated' => 1};

    # initialize all the data arrays for every new value type
    foreach my $value_type ( @$value_types ) {

        $updates->{"values.$value_type"} = $self->_get_empty_data_array();

        # also mark it as being used
        $self->value_types->{$value_type} = 1;
    }

    my $query = Tie::IxHash->new( identifier => $self->measurement_identifier,
                                  start => $self->start,
                                  end => $self->end );

    return $aggregate_collection->update_one( $query, {'$set' => $updates} );
}

sub update {

    my ( $self, %args ) = @_;

    my $bulk = $args{'bulk'};
    
    my $aggregate_points = $self->aggregate_points;

    my $query = Tie::IxHash->new( identifier => $self->measurement_identifier,
                                  start => $self->start,
                                  end => $self->end );

    #my $updates = {'updated' => time()};
    my $updates = {'updated' => 1};

    my $min;
    my $max;

    foreach my $aggregate_point ( @$aggregate_points ) {

        my $value_type = $aggregate_point->value_type;
        my $value = $aggregate_point->value;
        my $time = $aggregate_point->time;

        $min = $time if (! defined $min || $time < $min);
        $max = $time if (! defined $max || $time > $max);

        my $indexes = $self->get_indexes( $time );

        my ( $x, $y, $z ) = @$indexes;

        $updates->{"values.$value_type.$x.$y.$z"} = $value;
    }

    # doing this as part of a bulk operation?
    if ( $bulk ) {

	$bulk->find( $query )->update_one( {'$set' => $updates,
                                            '$min' => {'updated_start' => $min},
                                            '$max' => {'updated_end'   => $max}} );
        return;
    }

    # single doc update
    else {

	my $aggregate_collection = $self->data_type->database->get_collection( "data_" . $self->interval );

	$aggregate_collection->update_one( $query, {'$set' => $updates,
                                                    '$min' => {'updated_start' => $min},
                                                    '$max' => {'updated_end'   => $max} } );
        return 1;
    }
}

sub create {

    my ( $self, %args ) = @_;

    my $bulk = $args{'bulk'};

    my $interval = $self->interval;

    my $values = {};

    my $value_types = $self->data_type->value_types;
    my $aggregate_points = $self->aggregate_points;

    # first, initialize all the aggregate arrays for every known value type
    foreach my $value_type ( keys( %$value_types ) ) {

        $values->{$value_type} = $self->_get_empty_data_array();
    }

    my $updated_start;
    my $updated_end;

    # now handle every aggregate point to determine the proper update for the document
    foreach my $aggregate_point ( @$aggregate_points ) {

        my $value_type = $aggregate_point->value_type;
        my $time = $aggregate_point->time;
        my $value = $aggregate_point->value;

        $updated_start = $time if ( !defined( $updated_start ) || $time < $updated_start );
        $updated_end = $time if ( !defined( $updated_end ) || $time > $updated_end );

        # determine the index(es) of this aggregate point
        my $indexes = $self->get_indexes( $time );

        my ( $x, $y, $z ) = @$indexes;

        $values->{$value_type}[$x][$y][$z] = $value;
    }

    my $now = time();

    my $fields = Tie::IxHash->new( identifier => $self->measurement_identifier,
                                   start => $self->start,
                                   end => $self->end,
                                   interval => $interval,
                                   #updated => $now,
				   updated => 1,
                                   updated_start => $updated_start,
                                   updated_end => $updated_end,
                                   values => $values );

    if ( $bulk ) {

	$bulk->insert_one( $fields );
    }

    else {

	my $aggregate_collection = $self->data_type->database->get_collection( "data_$interval" );
	$aggregate_collection->insert_one( $fields );
    }

    return $self;
}

sub fetch {

    my ( $self, %args ) = @_;

    my $data = $args{'data'};
    my $interval = $self->interval;

    my $aggregate_collection = $self->data_type->database->get_collection( "data_$interval" );

    my $fields = Tie::IxHash->new( identifier => $self->measurement_identifier,
                                   start => $self->start,
                                   end => $self->end );

    # grab live document from mongo
    my $live_data = $aggregate_collection->find_one( $fields );

    # doc doesn't exist
    if ( !$live_data ) {

        return;
    }

    # parse all the aggregate point values of this document according to their timestamp
    my $data_values = $live_data->{'values'};
    my @value_types = keys( %$data_values );
    my $value_types = {};

    my @aggregate_points;

    foreach my $value_type ( @value_types ) {

        $value_types->{$value_type} = 1;

        my $time = $live_data->{'start'};

        if ( $data ) {

            # flatten the values array, might be 1D (old data, maintain backward compatibility) or 3D
            my @values = List::Flatten::Recursive::flat( $data_values->{$value_type} );

            foreach my $value ( @values ) {

                my $aggregate_point = GRNOC::TSDS::AggregatePoint->new( data_type => $self->data_type,
                                                                        value_type => $value_type,
                                                                        time => $time,
                                                                        value => $value,
                                                                        interval => $interval );

                push( @aggregate_points, $aggregate_point);

                $time += $interval;
            }
        }
    }

    $self->aggregate_points( \@aggregate_points ) if $data;
    $self->value_types( $value_types );

    return $self;
}

sub get_indexes {

    my ( $self, $time ) = @_;

    my $start = $self->start;
    my $end = $self->end;
    my $interval = $self->interval;
    my $dimensions = $self->dimensions;

    # align time to interval
    $time = int( $time / $interval ) * $interval;

    my $diff = ( $time - $start ) / $interval;

    my ( $size_x, $size_y, $size_z ) = @$dimensions;

    my $x = int( $diff / ( $size_y * $size_z ) );
    my $remainder = $diff - ( $size_y * $size_z * $x );
    my $y = int( $remainder / $size_z );
    my $z = $remainder % $size_z;

    return [$x, $y, $z];
}

### private methods ###

sub _trigger_aggregate_points {

    my ( $self, $aggregate_points ) = @_;

    # update all value types
    my $value_types = {};

    foreach my $aggregate_point ( @$aggregate_points ) {

        $value_types->{$aggregate_point->value_type} = 1;
    }

    $self->value_types( $value_types );
}

sub _get_empty_data_array {

    my ( $self ) = @_;

    my $array = [];

    my $dimensions = $self->dimensions;

    my ( $size_x, $size_y, $size_z ) = @$dimensions;

    for ( my $i = 0; $i < $size_x; $i++ ) {

        for ( my $j = 0; $j < $size_y; $j++ ) {

            for ( my $k = 0; $k < $size_z; $k++ ) {

                $array->[$i][$j][$k] = undef;
            }
        }
    }

    return $array;
}

1;
