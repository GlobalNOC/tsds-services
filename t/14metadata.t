use strict;
use warnings;

use Test::More tests => 84;

use GRNOC::Config;
use GRNOC::TSDS::DataService::MetaData;

use GRNOC::Log;

use JSON::XS;
use MongoDB;
use FindBin;
use Data::Dumper;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";

GRNOC::Log->new(config => $logging_file);

my $meta_ds = GRNOC::TSDS::DataService::MetaData->new(config_file => $config_file);

# We're going to connect to Mongo manually to generate specific measurement
# metadata documents so that it's easier to test this in a vacuum
my $config     = GRNOC::Config->new(config_file => $config_file, force_array => 0);
my $mongo_host = $config->get( '/config/mongo/@host' );
my $mongo_port = $config->get( '/config/mongo/@port' );
my $user       = $config->get( "/config/mongo/readwrite" );

my $mongo = MongoDB::MongoClient->new(
    host     => "mongodb://$mongo_host:$mongo_port",
    username => $user->{'user'},
    password => $user->{'password'}
);

my $testdb    = $config->get( '/config/unit-test-database' );

my $measurements = $mongo->get_database($testdb)->get_collection("measurements");

if (! $measurements){
    BAIL_OUT("Unable to get measurements collection in interface");
}

my $IDENTIFIER = "70fe20cfe624a4c7dc7617a6e0febbe2d16ee5012699566b50fde3b2d78a219e";

my $metadata = {
    node        => "rtr.foo.bar",
    intf        => "intf1",
    identifier  => $IDENTIFIER,
    start       => 100,
    end         => undef
};

$measurements->remove({"identifier" => $IDENTIFIER});

$measurements->insert($metadata);

# identifier isn't used anymore and causes errors below
delete $metadata->{'identifier'};

# Okay first a bunch of fail updates. These are bad for one reason or another
my $res;
my $docs;

# Not an array
$res = $meta_ds->update_measurement_metadata(values => {"this" => "is bad"}, type_field => 'type');
ok(! defined $res && $meta_ds->error() =~ /must be an array/, "update failed due to wrong data structure");

# Elements aren't objects
$res = $meta_ds->update_measurement_metadata(values => [1], type_field => 'type');
ok(! defined $res && $meta_ds->error() =~ /must be an array/, "update failed due to wrong data structure");

# Missing type
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(! defined $res && $meta_ds->error() =~ /missing required field \"type\"/, "update failed due to missing type");

# Bad type
$metadata->{'type'} = '________not_present';
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(! defined $res && $meta_ds->error() =~ /Unknown database/, "update failed due to bad type");

# Bad type_field
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'not_existing_type_field');
ok(! defined $res && $meta_ds->error() =~ /indicate which type of measurement this is/, "Missing type field");

# Set good type now
$metadata->{'type'} = $testdb;

# Missing start
my $start = delete $metadata->{'start'};
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(! defined $res && $meta_ds->error() =~ /required field \"start\"/, "update failed due to missing start");
$metadata->{'start'} = $start;

# Missing end
my $end = delete $metadata->{'end'};
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(! defined $res && $meta_ds->error() =~ /required field \"end\"/, "update failed due to missing end");
$metadata->{'end'} = $end;

# Missing required metadata
my $node = delete $metadata->{'node'};
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(! defined $res && $meta_ds->error() =~ /missing required field \"node\" for type/, "update failed due to missing required metadata");
$metadata->{'node'} = $node;

# Unknown metadata field
$metadata->{'unknown_field'} = 'new_value';
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(! defined $res && $meta_ds->error() =~ /Invalid metadata field/, "update failed due to having undocumented metadata");
delete $metadata->{'unknown_field'};


# Bad values field
$metadata->{'values'} = 'not a hash';
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(! defined $res && $meta_ds->error() =~ /values must be a hash of value/, "update failed due to bad 'values' field");

# Missing values keys
$metadata->{'values'} = {"not_a_value" => {}};
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(! defined $res && $meta_ds->error() =~ /Unknown values field/, "update failed due to unknown value type");

# Bad min key
$metadata->{'values'} = {"input" => {"min" => 'cat', "max" => 5}};
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(! defined $res && $meta_ds->error() =~ /must be an integer/, "update failed due to bad min values field");
delete $metadata->{'values'};

# Okay, enough bad sanity checking, let's test some real stuff.

# First a simple case - let's send the same thing.
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(defined $res, "got positive response back");
is($res->[0]{'modified'}, 0, "modified 0 documents");

$docs = _get_all_docs();
is(@$docs, 1, "still only 1 doc");

# The first incarnation is end = null, so we'll test updating "now"
$metadata->{'start'} = 150;
$metadata->{'end'}   = undef;
$metadata->{'description'} = 'new description';

$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(defined $res, "got positive response back");

is($res->[0]{'modified'}, 1, "modified 1 document");

$docs = _get_all_docs();
is(@$docs, 2, "now have 2 measurement docs");
is($docs->[0]{'start'}, 100, "original start time");
is($docs->[0]{'end'}, 150, "original doc now has end time");
is($docs->[1]{'start'}, 150, "new doc has start time");
is($docs->[1]{'end'}, undef, "new doc has null end time");


# Now let's try another update. This is going to push back the
# first doc to 10-12 and the current doc to 12-null.
$metadata->{'start'} = 120; # this goes in between the other two docs
$metadata->{'end'}   = undef;

$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'type');
ok(defined $res, "got positive response back");

is($res->[0]{'modified'}, 2, "modified 2 document");

$docs = _get_all_docs();
is(@$docs, 3, "now have 3 measurement docs");
is($docs->[0]{'start'}, 100, "original start time");
is($docs->[0]{'end'}, 120, "original doc now has pushed back end time");
is($docs->[1]{'start'}, 120, "new fragged doc has start time");
is($docs->[1]{'end'}, 150, "new fragged doc has end time");
is($docs->[2]{'start'}, 150, "existing doc has same start time");
is($docs->[2]{'end'}, undef, "existing doc has same end time");


### Change the "type_field" for all future tests, should have no bearing
### on any of the results
# Same thing but changing the type_field argument
$metadata->{'separate_type_field'} = delete $metadata->{'type'};

# Try an update that complete covers the middle document, this should
# do basically an in-place update
$metadata->{'start'} = 120;
$metadata->{'end'}   = 150;
$metadata->{'description'} = "second description";

$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'separate_type_field');
ok(defined $res, "got positive response back");

is($res->[0]{'modified'}, 2, "modified 2 document");

$docs = _get_all_docs();
is(@$docs, 3, "still have 3 measurement docs");
is($docs->[1]{'start'}, 120, "original start time");
is($docs->[1]{'end'}, 150, "original doc now has pushed back end time");
is($docs->[1]{'description'}, "second description", "new description");


# Try an update that fragments on the right side of the first document
$metadata->{'start'} = 100;
$metadata->{'end'}   = 110;
$metadata->{'description'} = 'third description';

$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'separate_type_field');
ok(defined $res, "got positive response back");

is($res->[0]{'modified'}, 2, "modified 2 documents");

$docs = _get_all_docs();
is(@$docs, 4, "have 4 measurement docs");
is($docs->[0]{'start'}, 100, "original start time");
is($docs->[0]{'end'}, 110, "original doc now has pushed back end time");
is($docs->[0]{'description'}, "third description", "same description");
is($docs->[1]{'start'}, 110, "new doc start time");
is($docs->[1]{'end'}, 120, "new doc end time");
is($docs->[1]{'description'}, undef, "still has no description");
is($docs->[2]{'start'}, 120, "third doc start time");


# Try to update an array'd field to make sure the sorting and
# comparisons / merging work okay
# First a few error checks for array fields
# Check for all same type verification
$metadata->{'tags'} = ["foo", "bar", {"this" => "object"}];
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'separate_type_field');
ok(! defined $res && $meta_ds->error() =~ /Not all values in array are of same type/, "got error about invalid subfields");
delete $metadata->{'tags'};

# Check for ensuring it's an array based on metadata spec
$metadata->{'circuit'} = 1;
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'separate_type_field');
ok(! defined $res && $meta_ds->error() =~ /must be an array/, "got error about wrong type");

# Check for subfield verification
$metadata->{'circuit'} = [{blah => 1}];
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'separate_type_field');
ok(! defined $res && $meta_ds->error() =~ /Invalid metadata field/, "got error about invalid subfields");


# Okay now a real one
$metadata->{'circuit'} = [{name => "circuit1", description => "circuit1", type => "10GE"},
                          {name => "circuit2", description => "circuit2", type => "ENET"}];

$metadata->{'start'} = 200;
$metadata->{'end'}   = undef;


$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'separate_type_field');
ok(defined $res, "got positive response back");
is($res->[0]{'modified'}, 1, "modified 1 document");

$docs = _get_all_docs();
is(@$docs, 5, "have 5 measurement docs");
ok(@{$docs->[4]{'circuit'}} == 2, "2 circuits listed on active document");
is($docs->[4]{'start'}, 200, "correct new start time");
is($docs->[4]{'end'}, undef, "correct new end");
is($docs->[3]{'end'}, 200, "correct previous end time");

# Now that the circuits are set, let's do an update but sending them in a
# different order. It should properly sort the results according to some random scalar key
# and detect that they're the same
$metadata->{'circuit'} = [{name => "circuit2", description => "circuit2", type => "ENET"},
                          {name => "circuit1", description => "circuit1", type => "10GE"}];

$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'separate_type_field');
ok(defined $res, "got positive response back");
is($res->[0]{'modified'}, 0, "realized arrays were the same content");


# Now do an update with a 3rd circuit - it should do a complete replace of
# the array
$metadata->{'circuit'} = [{name => "circuit2", description => "circuit2", type => "ENET"},
                          {name => "circuit3", description => "circuit3", type => "100GE"}];
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'separate_type_field');
ok(defined $res, "got positive response back");
is($res->[0]{'modified'}, 1, "updated 1 doc");

$docs = _get_all_docs();
is(@$docs, 5, "have 5 measurement docs");
ok(@{$docs->[4]{'circuit'}} == 2, "2 circuits listed on active document");
ok(! grep({$_->{'name'} eq 'circuit1'} @{$docs->[4]{'circuit'}}), "circuit 1 is gone");
ok(grep({$_->{'name'} eq 'circuit3'} @{$docs->[4]{'circuit'}}), "circuit 3 present");
ok(grep({$_->{'name'} eq 'circuit2'} @{$docs->[4]{'circuit'}}), "circuit 2 still there");


# Update values for a measurement
$metadata->{'values'} = {'input' => {'min' => 0, 'max' => 100},
			 'output' => {'min' => 5, 'max' => undef}};
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'separate_type_field');
ok(defined $res, "got positive response back");
is($res->[0]{'modified'}, 1, "updated 1 doc");

$docs = _get_all_docs();
is(@$docs, 5, "have 5 measurement docs");
my $values = $docs->[4]{'values'};
ok(keys %$values == 2, "2 values listed");
ok($values->{'input'}{'min'} == 0, "input min is good");
ok($values->{'input'}{'max'} == 100, "input max is good");
ok($values->{'output'}{'min'} == 5, "output min is good");
ok(exists $values->{'output'}{'max'} && ! defined $values->{'output'}{'max'}, "output max is good");

# Change the time and do another update, should cause a new instance
$metadata->{'start'} = 250;
$metadata->{'values'} = {'input' => {'min' => 0, 'max' => 100},
			 'output' => {'min' => undef, 'max' => 4000},
			 'status' => {'min' => 0, 'max' => 1}};
$res = $meta_ds->update_measurement_metadata(values => [$metadata], type_field => 'separate_type_field');
ok(defined $res, "got positive response back");
is($res->[0]{'modified'}, 1, "updated 1 doc");

$docs = _get_all_docs();

is(@$docs, 6, "have 6 measurement docs");
$values = $docs->[5]{'values'};
ok(keys %$values == 3, "3 values listed");
ok($values->{'input'}{'min'} == 0, "input min is good");
ok($values->{'input'}{'max'} == 100, "input max is good");
ok(exists $values->{'output'}{'min'} && ! defined $values->{'output'}{'min'}, "output min is good");
ok($values->{'output'}{'max'} == 4000, "output max is good");
ok($values->{'status'}{'min'} == 0, "status min is good");
ok($values->{'status'}{'max'} == 1, "status max is good");



sub _get_all_docs {
    my @fetched;

    my $cursor = $measurements->find({"identifier" => $IDENTIFIER});

    while (my $doc = $cursor->next()){
        push(@fetched, $doc);
        
    }

    # Sort them by start,end but treat undef end as really high
    # so that we get a full path from beginning to current
    my @sorted = sort {$a->{'start'} <=> $b->{'start'}
                       ||
                       (defined($a->{'end'}) ? $a->{'end'} : 999999) <=> (defined($b->{'end'}) ? $b->{'end'} : 999999) } @fetched;

    return \@sorted;   
}

