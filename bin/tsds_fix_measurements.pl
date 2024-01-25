#!/usr/bin/perl

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';
use lib './venv/lib/perl5';

use GRNOC::Config;

use MongoDB;
use Redis;
use Redis::DistLock;
use Getopt::Long;

use Data::Dumper;

my $USAGE = "$0 [--doit <actually make changes, not the default>]";

my $config_file = "/etc/grnoc/tsds/services/config.xml";
my $target_measurement = "";
my $target_hostname    = "";
my $doit        = 0;

GetOptions("c|config=s", \$config_file,
           "t|target_measurement=s", \$target_measurement,
           "n|target_hostname=s", \$target_hostname,
	   "doit", \$doit) or die;

if (! $doit){
    print "**  Not actually making changes since --doit was not provided **\n";
}

my $config  = GRNOC::Config->new(config_file => $config_file);
my $rw_user = $config->get('/config/mongo/readwrite/@user')->[0];
my $rw_pass = $config->get('/config/mongo/readwrite/@password')->[0];
my $host    = $config->get('/config/mongo/@host')->[0];
my $port    = $config->get('/config/mongo/@port')->[0];

my $mongo = MongoDB::MongoClient->new(host     => "$host:$port", 
				      username => $rw_user,
				      password => $rw_pass);

my $redis_host = $config->get('/config/redis/@host')->[0];
my $redis_port = $config->get('/config/redis/@port')->[0];

my $redis  = Redis->new( server => "$redis_host:$redis_port" );
my $locker = Redis::DistLock->new( servers => [$redis],
				   retry_count => 100,
				   retry_delay => 0.1);

my @dbs = $mongo->database_names();
my @exclude_dbs = ("admin", "config");


# Scan every database
foreach my $db_name (@dbs){

    if ( $db_name ~~ @exclude_dbs ) { next; }
    if ( $target_measurement ne "" && $target_measurement ne $db_name ) { 
      print "Skipping $db_name\n";
      next;
    }
    print "Checking $db_name\n";

    my $db               = $mongo->get_database($db_name);
    my $measurements_col = $db->get_collection('measurements');
    # Grab the unique set of identifiers, ie the unique series of
    # measurement metadata
    my $query_result;
    if ( $target_hostname ne "" ) { 
      $query_result = $measurements_col->aggregate(
          [  
             { '$match' => { 'node' => "$target_hostname" } },
             { '$group' => { '_id' => '$identifier' } } 
          ] ) ;
    }else {
      $query_result = $measurements_col->aggregate(
          [  
             { '$group' => { '_id' => '$identifier' } } 
          ] ) ;
    }
    my @tmp_uniq_ids;
    my $unique_identifiers;

    while( my $qr = $query_result->next ) {
        push @tmp_uniq_ids, $qr->{'_id'};
    }
    next if ( ! @tmp_uniq_ids ); 
    $unique_identifiers = \@tmp_uniq_ids;

    print "$db_name: sizeof unique identifiers = " . scalar(@$unique_identifiers) . "\n";

    my $num_bad = 0;

    # Go through each identifier and figure out if this measurement
    # has any overlapping problems
    foreach my $identifier (@$unique_identifiers){

	# Make the same lock that the rest of the processes do to make sure
	# we don't encounter the same race problem
	my $lock;
	my $retries = 5;
	while ($retries-- > 0 && ! $lock){
	    $lock = $locker->lock("lock__" . $db_name . "__measurements__" . $identifier, 30);
	}

	if (! $lock){
	    print "Unable to lock $identifier measurements, exiting\n";
	    exit(1);
	}

	my @measurements = $measurements_col->find({"identifier" => $identifier})->all();

	#print  "    $identifier => " . scalar(@measurements) . "\n";

	# can't be an overlap or multiple active with a single measurement, pass
	if (@measurements == 1){
	    $locker->release($lock);
	    next;
	}

	# Sort by start ascending, then scan each in order
	# marking where the last ended and if we ever go backwards
	# we have an overlap	
	@measurements = sort { int($a->{'start'}) <=> int($b->{'start'}) 
				   || 
			       (defined($b->{'end'}) ? $b->{'end'} : 999999999999) <=> (defined($a->{'end'}) ? $a->{'end'} : 999999999999)
	                    } @measurements;
		       

	# also keep track of number of in service we have seen,
	# should always be <= 1
	my $active = 0;

	my $is_bad = 0;

	my $count = 0;

	# type fixing on the start/end times
	# not needed anymore, one time fix
	# foreach my $measurement (@measurements){
	#     $count++;

	#     # type fixing maybe
	#     $measurement->{'start'} = int($measurement->{'start'});
	#     $measurement->{'end'}   = int($measurement->{'end'}) if defined ($measurement->{'end'});
	    
	#     my $doc_id = $measurement->{'_id'};
	    
	#     # reinsert document now, booooo, only way to ensure the type fixing
	#     # above actually takes effect since perl can't easily test for string vs int
	#     if ($doit){
	# 	$measurements_col->remove({"_id" => $doc_id});
	# 	$measurements_col->insert_one($measurement);
	#     }	    	    
	# }


	my $last_end = $measurements[0]->{'end'};
	my $first    = shift @measurements;

	# have to check to see if the first one was active, somewhat of an edge 
	# case where only two docs exist and both are active
	if (! defined $last_end){
	    $active = 1;
	    if (@measurements > 0){
		print "First doc was active for $identifier, but we found other docs with later start times\n";
	    }
	}

	foreach my $measurement (@measurements){

	    my $doc_id = $measurement->{'_id'};
	    
	    # if we already saw the active document and we're still
	    # scanning, we have a problem, it should always be last
	    if ($active){
		$is_bad = 1;
		
		# since we sorted by start time, we will have already passed the "good"
		# active doc, so we can just axe this one
		if ($doit){
		    print "Already saw active doc for $identifier, removing!\n";
		    $measurements_col->remove({"_id" => $doc_id});
		}
		else {
		    print "Already saw active doc for $identifier\n";
		}
	    }

	    $active++ if (! defined $measurement->{'end'});

	    my $current_end   = $measurement->{'end'};
	    my $current_start = $measurement->{'start'};

	    # some really dumb basic sanity checking
	    if (! defined($current_start) || (defined $current_end && $current_start >= $current_end)){
		$is_bad = 1;

		if ($doit){
		    print "Bad doc $doc_id, missing data or start ($current_start) > end ($current_end), removing!\n";
		    $measurements_col->remove({"_id" => $doc_id});
		}
		else {
		    print "Bad doc $doc_id, missing data or start ($current_start) > end ($current_end)\n";
		}
		next;
	    }

	    # if this record overlaps with the last one, there's a problem
	    # (works because of sort above)
	    if ($current_start < $last_end){
		print "Start time $current_start less than last end $last_end for $identifier (count = $count)\n";
		$is_bad = 1;

		# two cases here, either just the start is less than the end
		# or BOTH are < the last end, in which case we just nuke it and move on
		if (defined $current_end && $current_end <= $last_end){
		    if ($doit){
			print "Doc $doc_id entirely  inside the last doc for $identifier, removing\n";
			$measurements_col->remove({"_id" => $doc_id});
		    }
		    else {
			print "Doc $doc_id entirely inside the last doc for $identifier\n";

			# hack ahcka chaksdlsadhf
			# my $had_null = 0;
			# foreach my $t (($first, @measurements)){
			#     print "$t->{'_id'} Start: $t->{'start'} End: $t->{'end'} Delta: " . ($t->{'end'} - $t->{'start'}) . "\n";
			#     $had_null = 1 if (! defined $t->{'end'});
			# }
			# sleep(5) if ($had_null);

		    }
		    next;
		}
		# just the start is off, we can manually fix that and reinsert
		else {
		    if ($doit){
			print "Start time on $doc_id for $identifier needs moving forward, adjusting\n";
			$measurements_col->remove({"_id" => $doc_id});
			$measurement->{'start'} = $last_end;
			$measurements_col->insert_one($measurement);
		    }
		    else {
			print "Start time on $doc_id for $identifier needs moving forward\n";
		    }
		}
	    }

	    # null end symbolizes still active, so have to be
	    # a bit more precise here
	    if (defined $current_end && $current_end <= $last_end){
		$is_bad = 1;

		if ($doit){
		    print "End time on $doc_id for $identifier needs moving forward, adjusting\n";
		    $measurements_col->remove({"_id" => $doc_id});
		    $measurement->{'end'} = $last_end;
		    $measurements_col->insert_one($measurement);
		}
		else {
		    print "End time on $doc_id for $identifier needs moving foward\n";
		}
	    }

	    # move our time pointer forward
	    $last_end = $current_end;
	}

	if ($is_bad){
	    $num_bad++;

	    if ($doit){
		print "Identifier $identifier had problems and was cleaned up\n";
	    }
	    else {
		print "Identifier $identifier had problems\n";
	    }
	}

	# now that we're done, release the redis lock on this measurement series
	$locker->release($lock);
    }

    print "  Total measurements with issues = $num_bad / " . scalar(@$unique_identifiers) . "\n";
}
