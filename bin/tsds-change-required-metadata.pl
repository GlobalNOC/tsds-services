#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5

use strict;
use warnings;

use Data::Dumper;
use GRNOC::Config;
use GRNOC::CLI;
use Getopt::Long;
use GRNOC::TSDS::Writer::DataMessage;
use GRNOC::TSDS::DataType;
use MongoDB;

# preserve options we don't parse straight away
# need to lazy-load the measurement-type specific ones
Getopt::Long::Configure("pass_through");

# flush output so progress is easier to track
$| = 1;

my $config_file = "/etc/grnoc/tsds/services/config.xml";
my $db_name;
my $help;
my %req_metadata;
my %metadata_from;
my %metadata_to;
my $confirm = 0; # safety first

GetOptions("d|database=s" => \$db_name,
	   "c|config=s"   => \$config_file,
	   "h|help"       => \$help,
	   "confirm"      => \$confirm);

die help() if ($help);
die help("Missing database argument") if (! defined $db_name);

if (! -e $config_file || ! -r $config_file){
    die help("Config file \"$config_file\" does not exist or is not readable.");
}

my $config    = GRNOC::Config->new(config_file => $config_file);
my $root_user = $config->get('/config/mongo/root/@user')->[0];
my $root_pass = $config->get('/config/mongo/root/@password')->[0];
my $host      = $config->get('/config/mongo/@host')->[0];
my $port      = $config->get('/config/mongo/@port')->[0];

my $mongo = MongoDB::MongoClient->new(host => "$host:$port",
                                      username => $root_user,
                                      password => $root_pass);

# Make sure db exists
if (! grep {$_ eq $db_name} $mongo->database_names()){
    die help("Unknown database \"$db_name\"");
}

my $database = $mongo->get_database($db_name);

# Grab the required metadata fields for this database
my $metadata = $database->get_collection('metadata')->find_one();
my $meta_fields = $metadata->{'meta_fields'};

foreach my $field (keys %$meta_fields){
    next if (! $meta_fields->{$field}->{'required'});
    
    $req_metadata{$field} = undef;
}

# This shouldn't be possible but just in case
die help("Unable to determine metadata fields for \"$db_name\", internal config error?") if (! keys %req_metadata);

my $data_type = GRNOC::TSDS::DataType->new(name => $db_name,
					   database => $database);
					   

# Now that we know what other fields we need, grab those from the options
foreach my $field (keys %req_metadata){
    my $val_from;
    my $val_to;
    GetOptions("from_" . $field . "=s" => \$val_from);
    GetOptions("to_" . $field . "=s" => \$val_to);    

    if (! $val_from || ! $val_to){
	die help("Missing to or from values for field \"$field\"");
    }

    # Specially remap these for mongo queries, makes it easier
    # to say migrate just a node name and pull every interface
    # along with it
    if ($val_from eq '*'){
	$val_from = {'$regex' => '.+'};
    }
    if ($val_to eq '*'){
	$val_to = {'$regex' => '.+'};
    }

    $metadata_to{$field} = $val_to;
    $metadata_from{$field} = $val_from;
}


# Okay NOW we can finally get started

# Step 1 - grab the original measurements
my $identifiers = $database->run_command(["distinct" => "measurements",
					  "key"      => "identifier",
					  "query"    => \%metadata_from
					 ])->{'values'};

die "No measurements found for " . Dumper(\%metadata_from) if (! @$identifiers);

print "Found " . scalar(@$identifiers) . " unique measurements to update\n";

foreach my $old_identifier (@$identifiers){

    my $doc = $database->get_collection('measurements')->find_one({"identifier" => $old_identifier});

    my %orig_metadata;
    foreach my $field (keys %req_metadata){
	$orig_metadata{$field} = $doc->{$field};
    }

    print "Original => " . Dumper(\%orig_metadata);

    # This is a bit awkward, we re-use the same library that TSDS writer
    # does to avoid duplicating its identifier-creation logic and to 
    # ensure it's exactly the same. Most of this is throw-away just to get there
    my $message = GRNOC::TSDS::Writer::DataMessage->new(data_type => $data_type,
							time => time(),
							interval => 1,
							values => {},
							meta => \%orig_metadata);

    # Dumb sanity check, should never fail
    if ($message->measurement_identifier ne $old_identifier){
	die "Calculated original identifier does not equal existing identifier, aborting\nOriginal = $old_identifier\nCalculated = " . $message->measurement_identifier;
    }

    # Now figure out what the metadata should be
    foreach my $field (keys %metadata_to){
	next if (ref $metadata_to{$field}); # skip the ones that were wildcarded
	$orig_metadata{$field} = $metadata_to{$field};
    }

    print "Changing To => " . Dumper(\%orig_metadata);

    my $new_message = GRNOC::TSDS::Writer::DataMessage->new(data_type => $data_type,
							    time => time(),
							    interval => 1,
							    values => {},
							    meta => \%orig_metadata);
    

    my $new_identifier = $new_message->measurement_identifier;
    print "Mapping $old_identifier to $new_identifier\n";


    # If we're in test mode, don't actually do anything.
    next if (! $confirm);


    # Okay we have the original identifier and the new identifier, now 
    # we can start updating documents.

    # First up we'll do the data documents
    foreach my $collection_name ( ( "data", "data_3600", "data_86400") ){
	_migrate_data($old_identifier, $new_identifier, $collection_name, $database);
    }

    # Now that data is done we can do measurements, then we're all set
    _migrate_metadata($old_identifier, $new_identifier, $database, \%orig_metadata);
}

if (! $confirm){
    print "Ending run without actually doing anything because --confirm not set\n";
    exit(0);
}

print "Restarting memcached...\n";
print `systemctl restart memcached`;

sub _migrate_data {
    my $old_identifier  = shift;
    my $new_identifier  = shift;
    my $collection_name = shift;
    my $database        = shift;
    
    my $old_data = $database->get_collection($collection_name)->find({"identifier" => $old_identifier});    
    my $data_count = $database->get_collection($collection_name)->count_documents({"identifier" => $old_identifier});    

    print "  Migrating $collection_name ($data_count docs)...\n";

    my $i = 0;
    while (my $old_doc = $old_data->next()){
	
	# check to see if we have any docs we need to merge in, the intervals are the same so should be exact matches
	my $old_start = $old_doc->{'start'};
	my $old_end   = $old_doc->{'end'};

	print ".";
	
	my $new_doc = $database->get_collection($collection_name)->find_one({"identifier" => $new_identifier, "start" => $old_start, "end" => $old_end});
	
	# If we had a new doc, we need to merge all the values together
	# This assumes they're always a 10x10x10 nested array structure
	if ($new_doc){
	    foreach my $old_key (keys %{$old_doc->{"values"}}){
		
		if (! exists $new_doc->{"values"}{$old_key}){
		    warn "New doc didn't have key $old_key, skipping";
		    next;
		}

		my $old_data_vals = $old_doc->{"values"}{$old_key};
		my $new_data_vals = $new_doc->{"values"}{$old_key};

		my $had_changes = 0;

		for (my $i = 0; $i < @$old_data_vals; $i++){
		    my $i_data = $old_data_vals->[$i];
		    for (my $j = 0; $j < @$i_data; $j++){
			my $j_data = $i_data->[$j];
			for (my $k = 0; $k < @$j_data; $k++){
			    my $old_k_data = $j_data->[$k];

			    # If we had an old value but not a new one, merge it forward
			    if (defined $old_k_data && !defined($new_data_vals->[$i][$j][$k])){
				$new_data_vals->[$i][$j][$k] = $old_k_data;
				$had_changes = 1;
			    }
			}
		    }
		}

		if ($had_changes){
		    # Make a note, might be useful in the future
		    $new_doc->{'__old_identifier'} = $old_identifier;
		    $database->get_collection($collection_name)->replace_one({_id => $new_doc->{'_id'}}, $new_doc);
		}

	    }

	}
	# If we did NOT have a new doc, we can just update the identifier, insert, and move forward
	else {
	    # rewrite the identifier field to our new hash
	    #print "    doing a simple move...\n";
	    $old_doc->{'identifier'} = $new_identifier;
	    $old_doc->{'__old_identifier'} = $old_identifier;
	    _replace_doc($database, $collection_name, $old_doc);
	}

    }
    print "done\n";
}

sub _migrate_metadata {
    my $old_identifier = shift;
    my $new_identifier = shift;
    my $database       = shift;
    my $metadata       = shift;

    my @old_measurements = $database->get_collection('measurements')->find({"identifier" => $old_identifier})->sort({"start" => 1})->all();
    my @new_measurements = $database->get_collection('measurements')->find({"identifier" => $new_identifier})->sort({"start" => 1})->all();

    print "  Migrating measurements...\n";

    # In a normal circumstance the "old" measurements will exist mostly before the "new"
    # measurements, so we need to merge them in accordingly
    my $earliest_new;
    if (@new_measurements){
	$earliest_new = $new_measurements[0]->{'start'};
    }

    foreach my $old_measurement (@old_measurements){
	$old_measurement->{'identifier'} = $new_identifier;

	# fix up the textual records as well as the identifier
	foreach my $key (keys %$metadata){
	    $old_measurement->{$key} = $metadata->{$key};
	}

	# If there was no new doc, this is easy. Just migrate the identifiers and 
	# be done with it
	if (! $earliest_new){
	    _replace_doc($database, "measurements", $old_measurement);
	    next;
	}

	# If we've passed the new record starting point, we're done with the
	# old records
	if ($old_measurement->{'start'} >= $earliest_new){
	    last;
	}
	
	# If the current old measurement overlaps with the earliest new one, we need
	# to truncate it to avoid overlapping metadata
	if (! defined $old_measurement->{'end'} || $old_measurement->{'end'} >= $earliest_new){
	    $old_measurement->{'end'} = $earliest_new;
	}
	
	# At this point we will at least be updating the identifier, possibly the times
	# as well
	_replace_doc($database, "measurements", $old_measurement);
    }
    
}

sub _replace_doc {
    my $database   = shift;
    my $collection = shift;
    my $doc        = shift;

    $database->get_collection($collection)->delete_one({_id => $doc->{'_id'}});
    $database->get_collection($collection)->insert_one($doc);
}

sub help {
    my $err = shift || "";
    my $usage = "This is a script designed to help change values on normally immutable metadata fields in TSDS.";
    $usage  .=  " A common use case for this might be a node getting renamed and wanting to maintain a clean history.\n";
    $usage  .=  "Usage: $0 [--help] [-c|--config <tsds services config>] [--confirm] -d|--database <tsds database name>";

    foreach my $field (sort keys %req_metadata){
	$usage .= " --from_" . $field . " <value>";
	$usage .= " --to_" . $field . " <value>";
    }    

    $usage .= "\nScript will not actually make changes unless --confirm is provided";

    return $err . "\n" . $usage . "\n";
}

