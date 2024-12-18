#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::RedisLock;

use Moo;
use Types::Standard qw( Str Int HashRef Object Maybe );

use GRNOC::Log;

use GRNOC::TSDS::Redis;
use Redis::DistLock;

use Try::Tiny;

has config => ( is => 'rwp' );

has redis => ( is => 'rwp' );

has locker => ( is => 'rwp' );

has lock_retries => ( is => 'rw',
		      isa => Int,
		      default => 120 );

has lock_timeout => ( is => 'rw',
		      isa => Int,
		      default => 20 );

sub BUILD {
    my ( $self ) = @_;

    try {
        my $redis_conn = new GRNOC::TSDS::Redis(config => $self->config);
        $self->_set_redis($redis_conn->redis);
    }
    catch {
        log_error("Error connecting to Redis: $_");
        die("Error connecting to Redis: $_");
    };

    log_debug('Creating locker.');
    my $locker = Redis::DistLock->new(
        servers => [$self->redis],
        retry_count => $self->lock_retries(),
        retry_delay => 0.5
    );
    $self->_set_locker($locker);

    return $self;    
}

sub lock {
    my ( $self, %args ) = @_;

    my $cache_id = $self->get_cache_id( %args );
    my $lock_id = "lock__$cache_id";

    log_debug( "Getting lock id $lock_id." );

    return $self->locker->lock($lock_id, $self->lock_timeout());
}

sub unlock {
    my ( $self, $lock ) = @_;

    return $self->locker->release($lock);
}

sub get_cache_id {
    my ( $self, %args ) = @_;
    
    my $type = $args{'type'};
    my $collection = $args{'collection'};
    my $identifier = $args{'identifier'};
    my $start = $args{'start'};
    my $end = $args{'end'};
    
    my $id = $type . '__' . $collection;

    # include identifier in id if its given
    if ( defined( $identifier ) ) {	
        $id .= '__' . $identifier;
    }

    if ( defined( $start ) || defined( $end ) ) {
        $id .= '__' . $start;
        $id .= '__' . $end;
    }

    log_debug( "Getting cache id $id." );

    return $id;
}

1;
