#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::Writer;

use Moo;
use Types::Standard qw( Str Bool );

use GRNOC::TSDS::Config;
use GRNOC::TSDS::Writer::Worker;
use GRNOC::Config;
use GRNOC::Log;

use Parallel::ForkManager;
use Proc::Daemon;

use Data::Dumper;

### required attributes ###

has config_file => ( is => 'ro',
                     isa => Str,
                     required => 1 );

has logging_file => ( is => 'ro',
                      isa => Str,
                      required => 1 );

### optional attributes ###

has daemonize => ( is => 'ro',
                   isa => Bool,
                   default => 1 );

### private attributes ###

has config => ( is => 'rwp' );

has logger => ( is => 'rwp' );

has children => ( is => 'rwp',
                  default => sub { [] } );

### constructor builder ###

sub BUILD {

    my ( $self ) = @_;

    # create and store logger object
    my $grnoc_log = GRNOC::Log->new( config => $self->logging_file );
    my $logger = GRNOC::Log->get_logger();

    $self->_set_logger( $logger );

    # create and store config object
    my $config = GRNOC::Config->new( config_file => $self->config_file,
                                     force_array => 0 );

    $self->_set_config( $config );

    return $self;
}

### public methods ###

sub start {

    my ( $self ) = @_;

    $self->logger->info( 'Starting.' );

    $self->logger->debug( 'Setting up signal handlers.' );

    # setup signal handlers
    $SIG{'TERM'} = sub {

        $self->logger->info( 'Received SIG TERM.' );
        $self->stop();
    };

    $SIG{'HUP'} = sub {

        $self->logger->info( 'Received SIG HUP.' );
    };

    # need to daemonize
    if ( $self->daemonize ) {

        $self->logger->debug( 'Daemonizing.' );

        my $daemon = Proc::Daemon->new( pid_file => $self->config->get( '/config/pid-file' ) );

        my $pid = $daemon->Init();

        # in child/daemon process
        if ( !$pid ) {

            $self->logger->debug( 'Created daemon process.' );

            # change process name
            $0 = "tsds_writer";

            $self->_create_workers();
        }
    }

    # dont need to daemonize
    else {

        $self->logger->debug( 'Running in foreground.' );

        $self->_create_workers();
    }

    return 1;
}

sub stop {

    my ( $self ) = @_;

    $self->logger->info( 'Stopping.' );

    my @pids = @{$self->children};

    $self->logger->debug( 'Stopping child worker processes ' . join( ' ', @pids ) . '.' );

    return kill( 'TERM', @pids );
}

### helper methods ###

sub _build_config {

    my ( $self ) = @_;

    $self->logger->debug( 'Building GRNOC::Config with config file ' . $self->config_file . '.' );

    return GRNOC::Config->new( config_file => $self->config_file,
                               force_array => 0 );
}

sub _create_workers {

    my ( $self ) = @_;

    my $num_processes = $self->config->get( '/config/num-processes' );
    my $num_aggregate_processes = $self->config->get( '/config/num-aggregate-processes' );

    my $queue = $self->config->get( '/config/rabbit/@queue' );
    my $aggregate_queue = $self->config->get( '/config/rabbit/@aggregate-queue' );

    $self->logger->info( "Creating $num_processes high resolution and $num_aggregate_processes aggregate child worker processes." );

    my $total_workers = $num_processes + $num_aggregate_processes;

    my $config_v2 = new GRNOC::TSDS::Config(
        config_file => $self->config_file
    );

    my $forker = Parallel::ForkManager->new( $total_workers );

    # keep track of children pids
    $forker->run_on_start( sub {

        my ( $pid ) = @_;

        $self->logger->debug( "Child worker process $pid created." );

        push( @{$self->children}, $pid );
                           } );

    # create high res workers
    for ( 1 .. $num_processes ) {

        $forker->start() and next;

        # create worker in this process
        my $worker = GRNOC::TSDS::Writer::Worker->new( config => $config_v2,
                                                       logger => $self->logger,
						       queue => $queue );

        # this should only return if we tell it to stop via TERM signal etc.
        $worker->start();

        # exit child process
        $forker->finish();
    }

    # create aggregate workers
    for ( 1 .. $num_aggregate_processes ) {

        $forker->start() and next;

        # create worker in this process
        my $worker = GRNOC::TSDS::Writer::Worker->new( config => $config_v2,
                                                       logger => $self->logger,
						       queue => $aggregate_queue );

        # this should only return if we tell it to stop via TERM signal etc.
        $worker->start();

        # exit child process
        $forker->finish();
    }

    $self->logger->debug( 'Waiting for all child worker processes to exit.' );

    # wait for all children to return
    $forker->wait_all_children();

    $self->_set_children( [] );

    $self->logger->debug( 'All child workers have exited.' );
}

1;
