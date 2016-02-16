package GRNOC::TSDS::EventDocument;

use Moo;

use GRNOC::TSDS::Constants;
use GRNOC::TSDS::Event;

use Tie::IxHash;

use Data::Dumper;

### required attributes ###

has 'data_type' => ( is => 'ro',
                     required => 1 );

has 'type' => ( is => 'ro',
                required => 1 );

has 'start' => ( is => 'ro',
                 required => 1,
                 coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

has 'end' => ( is => 'ro',
               required => 1,
               coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

### optional attributes

has 'events' => ( is => 'rw',
                  required => 0,
                  trigger => 1,
                  default => sub { [] } );

has 'last_event_end' => ( is => 'rw',
                          required => 0,
                          coerce => sub { defined $_[0] ? $_[0] + 0 : undef } );

### constructor builder ###

sub BUILD {

    my ( $self ) = @_;

    # default the last_event_end to be the end date of the document
    $self->last_event_end( $self->end );

    return $self;
}

### public methods ###

sub add_event {

    my ( $self, $event ) = @_;

    push( @{$self->events}, $event );

    return $self->events;
}

sub update {

    my ( $self, %args ) = @_;

    my $bulk = $args{'bulk'};

    my $query = Tie::IxHash->new( type => $self->type,
                                  start => $self->start,
                                  end => $self->end );

    my $updates = {'updated' => time()};

    my $events = [];

    # handle every event that needs to be updated in this document
    foreach my $event ( @{$self->events} ) {

        my $start = $event->start;
        my $end = $event->end;
        my $identifier = $event->identifier;
        my $affected = $event->affected;
        my $text = $event->text;

        push( @$events, {'start' => $start,
                         'end' => $end,
                         'identifier' => $identifier,
                         'affected' => $affected,
                         'text' => $text} );
    }

    $updates->{'events'} = $events;
    $updates->{'last_event_end'} = $self->last_event_end;

    # doing this as part of a bulk operation?
    if ( $bulk ) {

        return $bulk->find( $query )->update( {'$set' => $updates} );
    }

    # single doc update
    else {

        my $event_collection = $self->data_type->database->get_collection( 'event' );

        return $event_collection->update( $query, {'$set' => $updates} );
    }
}

sub create {

    my ( $self, %args ) = @_;

    my $bulk = $args{'bulk'};

    my $values = {};
    my $events = [];

    # handle every event that needs to be included in this document
    foreach my $event ( @{$self->events} ) {

        my $start = $event->start;
        my $end = $event->end;
        my $identifier = $event->identifier,
        my $affected = $event->affected;
        my $text = $event->text;

        push( @$events, {'start' => $start,
                         'end' => $end,
                         'identifier' => $identifier,
                         'affected' => $affected,
                         'text' => $text} );
    }

    my $fields = Tie::IxHash->new( type => $self->type,
                                   start => $self->start,
                                   end => $self->end,
                                   last_event_end => $self->last_event_end,
                                   updated => time(),
                                   events => $events );

    # bulk op requested
    if ( $bulk ) {

	$bulk->insert( $fields );
    }

    # single op
    else {

	my $event_collection = $self->data_type->database->get_collection( 'event' );
	$event_collection->insert( $fields );
    }

    return $self;
}

sub fetch {

    my ( $self, %args ) = @_;

    my $event_collection = $self->data_type->database->get_collection( 'event' );

    my $query = Tie::IxHash->new( type => $self->type,
                                  start => $self->start,
                                  end => $self->end );

    # grab live document from disk
    my $live_data = $event_collection->find_one( $query );

    # doc doesn't exist
    if ( !$live_data ) {

        return;
    }

    # parse all the events of this document
    my $events = $live_data->{'events'};

    my @events;

    foreach my $event ( @$events ) {

        $event = GRNOC::TSDS::Event->new( data_type => $self->data_type,
                                          type => $self->type,
                                          start => $event->{'start'},
                                          end => $event->{'end'},
                                          identifier => $event->{'identifier'},
                                          text => $event->{'text'},
                                          affected => $event->{'affected'} );

        push( @events, $event );
    }

    $self->events( \@events );

    return $self;
}

### private methods ###

sub _trigger_events {

    my ( $self, $events ) = @_;

    # re-calculate the last_event_end whenever the events are modified
    my $last_event_end;

    foreach my $event ( @$events ) {

        my $event_end = $event->end;

        # this event doesn't have an end date set, so skip it
        next if ( !defined $event_end );

        # first event with end date we've seen
        if ( !defined $last_event_end ) {

            $last_event_end = $event_end;
        }

        # we've found an event with a greater end date than before
        elsif ( $event_end > $last_event_end ) {

            $last_event_end = $event_end;
        }
    }

    # default it to be the end date of the document if no event end dates found
    $last_event_end = $self->end if ( !defined( $last_event_end ) );

    $self->last_event_end( $last_event_end );
}

1;
