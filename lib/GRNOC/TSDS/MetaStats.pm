#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
package GRNOC::TSDS::MetaStats;

use Moo;
use Proc::Daemon;
use GRNOC::Config;
use GRNOC::Log;
use GRNOC::Counter;
use Net::AMQP::RabbitMQ;
use Tie::IxHash;
use Data::Dumper;
use JSON;
use LWP;
use LWP::Simple;
use LWP::UserAgent;
use Try::Tiny;
use List::MoreUtils qw( natatime );
use Types::Standard qw( Str Bool );
use Math::Round qw( nhimult );
use MongoDB;

use constant LOCK_TIMEOUT         => 120;
use constant QUEUE_PREFETCH_COUNT => 100;
use constant QUEUE_FETCH_TIMEOUT  => 10 * 1000000;
use constant RECONNECT_TIMEOUT    => 10;
use constant MAX_RATE_VALUE => 2199023255552;

use constant INTERVAL           => 60;
use constant SERVER_STATUS_TYPE => 'meta_tsds_server';
use constant SHARD_STATUS_TYPE  => 'meta_tsds_shard';
use constant DB_STATUS_TYPE     => 'meta_tsds_db';
use constant RABBIT_STATUS_TYPE => 'meta_tsds_rabbit';

### required attributes ###
has config_file => ( is => 'ro',
                     isa => Str,
                     required => 1 );

has logging_file => ( is => 'ro',
                      isa => Str,
                      required => 1 );



has daemonize => (is=>'ro',
		    isa=>Bool,
		  default=>1);


### RabbitMQ attribites
has rabbit => ( is => 'rwp' );

has rabbit_host => ( is => 'rwp' );

has rabbit_port => ( is => 'rwp' );

has rabbit_queue => ( is => 'rwp' );

### private attributes ###

has config => ( is => 'rwp' );

has logger => ( is => 'rwp' );

has mongo_rw => ( is => 'rwp' );

has counter => (is => 'rwp' );

my %counter_key = ();
my $counter;
my $collection_rate;

sub BUILD {

    my ( $self ) = @_;
    $counter = new GRNOC::Counter;
    $self->_set_counter( $counter );

    # create and store logger object
    my $grnoc_log = GRNOC::Log->new( config => $self->logging_file );
    my $logger = GRNOC::Log->get_logger();
    $self->_set_logger( $logger );

     # create and store config object
    my $config = GRNOC::Config->new( config_file => $self->config_file,
                                     force_array => 0 );
    $self->_set_config( $config );

    $self->{'refresh_rate'} = $config->get('/config/refresh-rate');
    $self->{'cache_refresh_rate'} = $config->get('/config/cache-refresh-rate');
    $self->{'last_cache_build'} = 0;

    return $self;
}

sub start {
    my ( $self ) = @_;

    $self->logger->info( 'Starting.' );

    # need to daemonize
    if ( $self->daemonize ) {

        $self->logger->debug( 'Daemonizing.' );
        my $daemon = Proc::Daemon->new( pid_file => $self->config->get( '/config/pid-file' ) );
        my $pid = $daemon->Init();

        # in child/daemon process
        if ( !$pid ) {
            $self->logger->debug( 'Created daemon process.' );

            # change process name
            $0 = "metastats_collector";
	 
	    $self->{'running'} = 1;
	    $self->_run();
	}
    }
    
    # dont need to daemonize
    else {
	
        $self->logger->debug( 'Running in foreground.' );
	$self->{'running'} = 1;
	$self->_run();
    }

}
sub _run(){
    my ($self) = @_;

    $self->logger->info("Starting.");

    $collection_rate = $self->{'refresh_rate'};
    while($self->{'running'}){
	my $now = time();

	if($now - $self->{'last_cache_build'} > $self->{'cache_refresh_rate'}){
	    
	    $self->logger->info("(Re)building the cache.");

	    my $rabbit_status = $self->_rabbit_connect();
	    my $db_status = $self->_mongodb_connect();
	    
	    if($rabbit_status and $db_status){
		$self->{'last_cache_build'} = $now;
	    }
	    else{
		$self->logger->error("Error connecting to rabbit or mongo. Continuing with old cache.");
	    }
	}

	my $timestamp = nhimult($self->{'refresh_rate'},$now);
	my $sleep_seconds = $timestamp - $now;

	my $human_readable_date = localtime($timestamp);

	$self->logger->info("Sleeping $sleep_seconds seconds till local time $human_readable_date ($timestamp).");

	while($sleep_seconds > 0){
	            
	    my $time_slept = sleep($sleep_seconds);

	    last if(!$self->{'running'});

	    $sleep_seconds -= $time_slept;
	}

	last if(!$self->{'running'});

	$self->_rabbit_status();                                                                                                      
        $self->_db_status();                                                                                                                   
        $self->_shard_status();   
    }
}

sub _rabbit_connect {

    my ( $self ) = @_;
    my $rabbit_host = $self->config->get( '/config/rabbit/@host' );
    my $rabbit_port = $self->config->get( '/config/rabbit/@port' );

    while ( 1 ) {
        $self->logger->info( "Connecting to RabbitMQ $rabbit_host:$rabbit_port." );
        my $connected = 0;
        try {
            my $rabbit = Net::AMQP::RabbitMQ->new();

            $rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );
            $rabbit->channel_open( 1 );

            $rabbit->basic_qos( 1, { prefetch_count => QUEUE_PREFETCH_COUNT } );


            $self->_set_rabbit( $rabbit );
            $connected = 1;
        }

        catch {

            $self->logger->error( "Error connecting to RabbitMQ: $_" );
        };

        last if $connected;

        $self->logger->info( "Reconnecting after " . RECONNECT_TIMEOUT . " seconds..." );
        sleep( RECONNECT_TIMEOUT );
    }
    return 1;
}

sub _mongodb_connect {

    my ( $self ) = @_;
  # connect to mongo                                                                                                                                                       
    my $mongo_host = $self->config->get( '/config/mongo/@host' );
    my $mongo_port = $self->config->get( '/config/mongo/@port' );
    my $rw_user    = $self->config->get( "/config/mongo/readwrite" );

    my $mongo;
    while ( 1 ) {
	$self->logger->debug( "Connecting to MongoDB as readwrite on $mongo_host:$mongo_port." );          
	my $connected = 0;
        try {
    	    $mongo = MongoDB::MongoClient->new( host => "$mongo_host");#:$mongo_port",                                                            
						#username => $rw_user->{'user'},                                                           
                      	#		        password => $rw_user->{'password'} );     
            $self->_set_mongo_rw( $mongo );
            $connected = 1;
        }
        catch {
	    $self->logger->error( "Error connecting to MongoDB: $_" );                                                                                                      
	    die( "Error connecting to MongoDB: $_" );  
        };

        last if $connected;

        $self->logger->info( "Reconnecting after " . RECONNECT_TIMEOUT . " seconds..." );
        sleep( RECONNECT_TIMEOUT );
    }
    return 1;
}

sub _rabbit_status {

    my ( $self ) = @_;
    
    $self->logger->debug("rabbitStatus"); 
    
    my $rabbit_host = $self->config->get( '/config/rabbit/@host' );
    my $rabbit_port = $self->config->get( '/config/rabbit/@port' );

    my $URI = 'http://'.$rabbit_host.':1'.$rabbit_port.'/api/nodes';
    my $user = $self->config->get( '/config/rabbit/@user' );
    my $pass = $self->config->get( '/config/rabbit/@password' );

    # define user agent                                                                          
    my $ua = LWP::UserAgent->new();
    $ua->agent("USER/AGENT/IDENTIFICATION");
    my $content;
    $self->logger->debug("rabbitStatus -- get");
    try{                                                                                                     
        # make request                                                                                                                        
	my $request = HTTP::Request->new(GET => $URI);
	$request->header( 'Content-Type' => 'application/json' );

	# authenticate                                                                                                                         
	$request->authorization_basic($user, $pass);

	# except response                                                                                                                       
	my $response = $ua->request($request);

	# get content of response                                                                                                              
	$content = $response->content();
    }                                                                                                                                     
    catch{
	$self->logger->error("Unable to get Rabbit node stats");                                                                               
    };  

    $self->logger->debug("rabbitStatus -- load");     
    my $response_message;
    try{	
        $response_message = decode_json($content);
    }
    catch{
	$self->logger->error("Unable to parse Rabbit node stats JSON.");
    };
    
    $response_message = @{$response_message}[0];
    my $name = $response_message->{'name'};
    my %out_message;
    tie %out_message, 'Tie::IxHash';
    my %metainfo;
    $metainfo{node} = $name;
    $out_message{interval} = INTERVAL;
    $out_message{meta} = \%metainfo;
    my $epoc = time();
    $out_message{time} = $epoc;
    $out_message{type} = 'meta_tsds_rabbit';

    my %values;
    tie %values, 'Tie::IxHash';

    my @list = ('fd_used', 'fd_total', 'sockets_used', 'sockets_total', 'mem_used', 'mem_limit', 'disk_free_limit', 'disk_free', 'proc_used', 'proc_total', 'run_queue');

    foreach my $field (@list){
	$values{$field} = $response_message->{$field}; 
    }

    $self->logger->debug("rabbitStatus -- get overview");
    
    $URI = 'http://'.$rabbit_host.':1'.$rabbit_port.'/api/overview?lengths_age=%s&lengths_incr=5&msg_rates_age=%s&msg_rates_incr=5';
    my $overview_request;
    my $overview_response;
    my $overview_content;

    try{
	$overview_request = HTTP::Request->new(GET => $URI);
	$overview_request->header( 'Content-Type' => 'application/json' );
	$overview_request->authorization_basic($user, $pass);
        $overview_response = $ua->request($overview_request);
	$overview_content = $overview_response->content();
    }
    catch{
	$self->logger->error("Unable to get Rabbit overview stats");
    };

    my $overview;
    try{
	$overview = decode_json($overview_content);
    }
    catch{
	$self->logger->error("Unable to parse Rabbit overview JSON.");
    };

    $self->logger->debug("a");
    
    $values{message_publish} = $overview->{'message_stats'}->{'publish_details'}->{'rate'};
    $values{message_ack} = $overview->{'message_stats'}->{'ack_details'}->{'rate'};
    $values{message_deliver} = $overview->{'message_stats'}->{'deliver_details'}->{'rate'};
    $values{message_redeliver} = $overview->{'message_stats'}->{'redeliver_details'}->{'rate'};

    $self->logger->debug("b");

    $values{queue_messages} = $overview->{'queue_totals'}->{'messages_details'}->{'rate'};
    $values{queue_messages_ready} = $overview->{'queue_totals'}->{'messages_ready_details'}->{'rate'};
    $values{queue_messages_unacknowledged} = $overview->{'queue_totals'}->{'messages_unacknowledged_details'}->{'rate'};
    
    $self->logger->debug("c");

    my $object_totals = $overview->{'object_totals'};
    while ( my ( $key, $value ) = each ( %$object_totals ) )
    {
	$values{'object_totals_'.$key} = $value;
    }

    $out_message{values}=\%values;
    $self->logger->debug("rabbitStatus -- done");
    
    #send rabbit stats to rabbitmq
    $self->_send_message_to_rabbit(\%out_message);
}

sub _db_status()
{
    my ( $self ) = @_;

    $self->logger->debug("dbStatus");

    # grab all database names in mongo
    my @database_names;
    try{
	@database_names = $self->mongo_rw->database_names();
    }
    catch{
	$self->logger->error("Unable to get database names ");
    };
    

    my %out_message;
    tie %out_message, 'Tie::IxHash';
    my %metainfo;

    $metainfo{db} = "";
    $metainfo{shard} = "";
    $out_message{interval} = INTERVAL;
    $out_message{meta} = \%metainfo; 
    my $epoc = time();
    $out_message{time} = $epoc;
    $out_message{type} = 'meta_tsds_db';

    foreach my $database ( @database_names ) {

	# skip it if its marked to be ignored 
	next if ( $database =~ /^_/ );
      	$metainfo{db} = $database;

	my $db =  $self->mongo_rw->get_database($database);
	my $result = $db->run_command({dbStats => 1 });
	$result = $result->{'raw'};

	while ( my ( $key, $value ) = each ( %$result ) )
	{
	    my %values;
	    tie %values, 'Tie::IxHash';
	    $metainfo{shard}= $key;
	    while ( my ( $key2, $value2 ) = each ( %$value ) )
	    {
		if($key2 ne 'db' and $key2 ne 'ok')
		{
		    $values{$key2}=$value2;
       		}
	    }
	    $out_message{values}=\%values;
	    
            #send db stats to rabbit
	    $self->_send_message_to_rabbit(\%out_message);    
	}

    }

}

sub _shard_status{
    my ( $self ) = @_;

    $self->logger->debug("shardStatus");
   
    my $database = "admin";
    my $db =  $self->mongo_rw->get_database($database);
    my $result = $db->run_command({listShards => 1 });
    my $shards = $result->{'shards'};
    
    my %out_message;
    tie %out_message, 'Tie::IxHash';
    my %metainfo;

    $metainfo{host} = "";
    $metainfo{shard}= "";
    $out_message{interval}=INTERVAL;
    $out_message{meta}=\%metainfo;
    my $epoc = time();
    $out_message{time}=$epoc;
    $out_message{type}='meta_tsds_shard';
    my $flag = 0;

    foreach my $shard (@{$shards}){
	my $host = $shard->{'host'};
	my $connection; 
	my %new_values = ();
	my %sum_values = ();
	my $timestamp = time();

	my %values;
	tie %values, 'Tie::IxHash';
	my $admin_db;
        try{
	    my $rw_user    = $self->config->get( "/config/mongo/readwrite" );
	    $connection = MongoDB::Connection->new(host => $host, username => $rw_user->{'user'}, password => $rw_user->{'password'} );
	    $admin_db = $connection->get_database('admin');  
	}
	catch{
	    $self->logger->debug("Unable to connect to Mongo ");
	};

	$metainfo{host} = $host;
	$metainfo{shard} = $shard->{'_id'};
	my $serverstats =  $admin_db->run_command({serverStatus => 1});
	my $asserts = $serverstats->{'asserts'};
	my $opcounters =  $serverstats->{'opcounters'};
	my $connections =  $serverstats->{'connections'};
	my $cursors =  $serverstats->{'cursors'};
	
	while ( my ( $key, $value ) = each ( %$asserts ) )
	{
	    $values{'asserts_'.$key} = $value;
	}
        while ( my ( $key, $value ) = each ( %$opcounters ) )
        {
            $values{'opcounters_'.$key} = $value;
        }
	while ( my ( $key, $value ) = each ( %$connections ) )
	{
	    $values{'connections_'.$key} = $value;
	}
	while ( my ( $key, $value ) = each ( %$cursors ) )
	{
            $values{'cursors_'.$key} = $value;
        }
	#note is a string deleting it
	delete $values{'cursors_note'};

	while ( my ( $key, $value ) = each ( %values ) )
	{
            my $rate;
	    my $new_key = $key.$shard->{'_id'};
	    if (!exists($counter_key{$new_key})) {
		$counter_key{$new_key} = 1;
		$self->counter->add_measurement($new_key, $collection_rate, -1, MAX_RATE_VALUE);
	    }

	    $rate = $self->counter->update_measurement($new_key, $timestamp, $value);
	    $rate = ($rate >= 0) ? $rate : undef;
	    
	    if (defined($rate) and ($rate >= 0)) {
		$sum_values{$new_key} += $rate;
		$new_values{$key} =  $sum_values{$new_key};
	    }
	}

	while ( my ( $key, $value ) = each ( %values ) )
	{
	    if (!exists($new_values{$key})) {
                $new_values{$key} = undef;
	    }	
	}

	$out_message{values} =\%new_values; 
	
        #Sending message to rabbit
	$self->_send_message_to_rabbit(\%out_message);
    }

}

sub _send_message_to_rabbit{
    my ($self,$out_messag) = @_;

    my %out_message = %{$out_messag};
    my $rabbit_messages = [];
    push @$rabbit_messages,\%out_message;

    try {
	if ( @$rabbit_messages > 0 ) {
	    my $queue = $self->config->get( '/config/rabbit/@queue' );
	    my $it = natatime( 100, @$rabbit_messages);
	    while ( my @messages = $it->() ) {
		$self->rabbit->publish( 1, $queue, encode_json( \@messages ), {'exchange' => ''} );
		print "Message Sent";
	    }
	}
    }
    catch {
	print "Error sending message to RabbitMQ: " ;
    };
}

sub stop {
    my ( $self ) = @_;
    $self->logger->debug( 'Stopping.' );
    $self->{'running'} = 0; 
    return 1;
}


1;

