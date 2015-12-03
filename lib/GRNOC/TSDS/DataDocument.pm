package GRNOC::TSDS::DataDocument;

use Moo;

use GRNOC::TSDS::Constants;
use GRNOC::TSDS::DataPoint;

use Tie::IxHash;
use List::Flatten::Recursive;

use Data::Dumper;

### required attributes ###

has 'data_type' => ( is => 'ro',
                     required => 1 );

has 'measurement_identifier' => ( is => 'ro',
                                  required => 1 );

has 'interval' => ( is => 'rw',
                    required => 0,
                    coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

has 'start' => ( is => 'ro',
                 required => 1,
                 coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

has 'end' => ( is => 'ro',
               required => 1,
               coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

### optional attributes ###

has 'data_points' => ( is => 'rw',
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

sub add_data_point {

    my ( $self, $data_point ) = @_;

    push( @{$self->data_points}, $data_point );

    # mark value type as being present too
    $self->value_types->{$data_point->value_type} = 1;

    return $self->data_points;
}

sub add_value_types {

    my ( $self, $value_types ) = @_;

    my $data_collection = $self->data_type->database->get_collection( 'data' );

    my $updates = {'updated' => time()};

    # initialize all the data arrays for every new value type
    foreach my $value_type ( @$value_types ) {

        $updates->{"values.$value_type"} = $self->_get_empty_data_array();

        # also mark it as being used
        $self->value_types->{$value_type} = 1;
    }

    my $query = Tie::IxHash->new( identifier => $self->measurement_identifier,
                                  start => $self->start,
                                  end => $self->end );

    return $data_collection->update( $query, {'$set' => $updates} );
}

sub update {

    my ( $self ) = @_;

    my $data_collection = $self->data_type->database->get_collection( 'data' );
    my $data_points = $self->data_points;

    my $query = Tie::IxHash->new( identifier => $self->measurement_identifier,
                                  start => $self->start,
                                  end => $self->end );

    my $updates = {'updated' => time()};

    my $min;
    my $max;

    foreach my $data_point ( @$data_points ) {
	
	my $value_type = $data_point->value_type;
	my $value = $data_point->value;
	my $time = $data_point->time;

        $min = $time if (! defined $min || $time < $min);
        $max = $time if (! defined $max || $time > $max);

	my $indexes = $self->get_indexes( $time );
	
	my ( $x, $y, $z ) = @$indexes;
	
	$updates->{"values.$value_type.$x.$y.$z"} = $value;
    }

    return $data_collection->update( $query, {'$set' => $updates, 
                                              '$min' => {'updated_start' => $min},
                                              '$max' => {'updated_end'   => $max} } );
}

sub create {

    my ( $self, %args ) = @_;

    my $values = {};

    my $data_collection = $self->data_type->database->get_collection( 'data' );
    my $value_types = $self->data_type->value_types;
    my $data_points = $self->data_points;

    # first, initialize all the data arrays for every known value type
    foreach my $value_type ( keys( %$value_types ) ) {

        $values->{$value_type} = $self->_get_empty_data_array();
    }

    # now handle every data point to determine the proper update for the document
    foreach my $data_point ( @$data_points ) {

        my $value_type = $data_point->value_type;
        my $time = $data_point->time;
        my $value = $data_point->value;

        # determine the index(es) of this data point
        my $indexes = $self->get_indexes( $time );

        my ( $x, $y, $z ) = @$indexes;

        $values->{$value_type}[$x][$y][$z] = $value;
    }

    my $now = time();

    my $fields = Tie::IxHash->new( identifier => $self->measurement_identifier,
                                   start => $self->start,
                                   end => $self->end,
                                   interval => $self->interval,
                                   updated => $now,
                                   updated_start => $now,
                                   updated_end => $now,
                                   values => $values );

    $data_collection->insert( $fields );

    return $self;
}

sub replace {

    my ( $self, %args ) = @_;

    my $values = {};

    my $data_collection = $self->data_type->database->get_collection( 'data' );
    my $value_types = $self->data_type->value_types;
    my $data_points = $self->data_points;

    # first, initialize all the data arrays for every known value type
    foreach my $value_type ( keys( %$value_types ) ) {

        $values->{$value_type} = $self->_get_empty_data_array();
    }

    # now handle every data point to determine the proper update for the document
    foreach my $data_point ( @$data_points ) {

        my $value_type = $data_point->value_type;
        my $time = $data_point->time;
        my $value = $data_point->value;

        # determine the index(es) of this data point
        my $indexes = $self->get_indexes( $time );

        my ( $x, $y, $z ) = @$indexes;

        $values->{$value_type}[$x][$y][$z] = $value;
    }

    my $query = Tie::IxHash->new( identifier => $self->measurement_identifier,
                                  start => $self->start,
                                  end => $self->end );

    my $fields = Tie::IxHash->new( identifier => $self->measurement_identifier,
                                   start => $self->start,
                                   end => $self->end,
                                   interval => $self->interval,
                                   updated => time(),
                                   values => $values );

    $data_collection->update( $query, $fields );

    return $self;
}

sub fetch {

    my ( $self, %args ) = @_;

    my $data = $args{'data'};

    my $data_collection = $self->data_type->database->get_collection( 'data' );

    my $fields = Tie::IxHash->new( identifier => $self->measurement_identifier,
                                   start => $self->start,
                                   end => $self->end );

    # grab live document from mongo
    my $live_data = $data_collection->find_one( $fields );

    # doc doesn't exist
    if ( !$live_data ) {

        return;
    }

    # update the interval of this document
    $self->interval( $live_data->{'interval'} );

    # parse all the data point values of this document according to their timestamp
    my $data_values = $live_data->{'values'};
    my @value_types = keys( %$data_values );
    my $value_types = {};

    my @data_points;

    foreach my $value_type ( @value_types ) {

        $value_types->{$value_type} = 1;

        my $time = $live_data->{'start'};

        if ( $data ) {

            # flatten the values array, might be 1D (old data, maintain backward compatibility) or 3D
            my @values = List::Flatten::Recursive::flat( $data_values->{$value_type} );

            foreach my $value ( @values ) {

                my $data_point = GRNOC::TSDS::DataPoint->new( data_type => $self->data_type,
                                                              value_type => $value_type,
                                                              time => $time,
                                                              value => $value,
                                                              interval => $self->interval );

                push( @data_points, $data_point);

                $time += $self->interval;
            }
        }
    }

    $self->data_points( \@data_points ) if $data;
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

sub _trigger_data_points {

    my ( $self, $data_points ) = @_;

    # update all value types
    my $value_types = {};

    foreach my $data_point ( @$data_points ) {

        $value_types->{$data_point->value_type} = 1;
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
