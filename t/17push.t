use strict;
use warnings;

use Test::More tests => 18;

use GRNOC::Config;
use GRNOC::TSDS::DataService::Push;

use GRNOC::Log;

use JSON::XS;
use MongoDB;
use FindBin;
use Data::Dumper;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $logging_file = "$FindBin::Bin/conf/logging.conf";

my $config    = GRNOC::Config->new(config_file => $config_file, force_array => 0);
my $testdb    = $config->get( '/config/unit-test-database' );

GRNOC::Log->new(config => $logging_file);

my $push_ds = GRNOC::TSDS::DataService::Push->new(config_file => $config_file);

# Test push restrictions correctly parsed and interpreted
my $data;
my $data2;
my $result;

# 
# data pushes
#

# First we use a user NOT defined in the restricted users. Everything should
# go normally
$data   = _gen_data("foobar", "rtr.chic", "xe-1/0/0");
$result = $push_ds->_validate_message(JSON::XS::encode_json([$data]), "not_restricted_user");
ok($result, "unrestricted user allowed to push");

# Now we use a user that doesn't have access to the database
$data   = _gen_data("foobar", "rtr.chic", "xe-1/0/0");
$result = $push_ds->_validate_message(JSON::XS::encode_json([$data]), "foo-user");
ok(! $result, "restricted user NOT allowed to push to disallowed DB");
ok($push_ds->error() =~ /not allowed to send data for type foobar/, "got correct error message");

# Now we send an update for something the user doesn't have metaata access to but 
# DOES have access to the measurement type
$data   = _gen_data("interface", "rtr.chic", "xe-1/0/0");
$result = $push_ds->_validate_message(JSON::XS::encode_json([$data]), "foo-user");
ok(! $result, "restricted user NOT allowed to push due to metadata patterns");
ok($push_ds->error() =~ /Metadata node = rtr.chic not allowed/, "got correct error message");

# Now we finally send something it does have access to to ensure it all works
$data   = _gen_data("interface", "rtr.foo.edu", "xe-1/0/0");
$result = $push_ds->_validate_message(JSON::XS::encode_json([$data]), "foo-user");
ok($result, "restricted user IS allowed to push when all constraints met");


# A few additional tests - make sure we check every update in the array. Any failure
# results in throwing away the whole message.
$data     = _gen_data("interface", "rtr.foo.edu", "xe-1/0/0");
$data2 = _gen_data("interface", "rtr.chic", "xe-1/0/0");
$result = $push_ds->_validate_message(JSON::XS::encode_json([$data, $data2]), "foo-user");
ok(! $result, "restricted user NOT allowed to push due to metadata patterns in multiple messages");
ok($push_ds->error() =~ /Metadata node = rtr.chic not allowed/, "got correct error message");


# Test whether a user with only database level constraints can push, ie no metadata restrictions
$data     = _gen_data("otherdb", "rtr.foo.edu", "xe-1/0/0");
$result = $push_ds->_validate_message(JSON::XS::encode_json([$data]), "foo-user");
ok($result, "restricted user IS allowed to push to database without metadata restrictions");



#
# metadata pushes
# TODO: make this not so copy/paste
#

# Test whether a user with only database level constraints can push a metadata udpate, ie no metadata restrictions
$data   = _gen_data("foobar.metadata", "rtr.chic", "xe-1/0/0", {"description" => "New description"});
$result = $push_ds->_validate_message(JSON::XS::encode_json([$data]), "not_restricted_user");
ok($result, "unrestricted user allowed to push");

# Now we use a user that doesn't have access to the database
$data   = _gen_data("foobar.metadata", "rtr.chic", "xe-1/0/0", {"description" => "New description"});
$result = $push_ds->_validate_message(JSON::XS::encode_json([$data]), "foo-user");
ok(! $result, "restricted user NOT allowed to push to disallowed DB");
ok($push_ds->error() =~ /not allowed to send data for type foobar/, "got correct error message");

# Now we send an update for something the user doesn't have metaata access to but 
# DOES have access to the measurement type
$data   = _gen_data("interface.metadata", "rtr.chic", "xe-1/0/0", {"description" => "New description"});
$result = $push_ds->_validate_message(JSON::XS::encode_json([$data]), "foo-user");
ok(! $result, "restricted user NOT allowed to push due to metadata patterns");
ok($push_ds->error() =~ /Metadata node = rtr.chic not allowed/, "got correct error message");

# Now we finally send something it does have access to to ensure it all works
$data   = _gen_data("interface.metadata", "rtr.foo.edu", "xe-1/0/0", {"description" => "New description"});
$result = $push_ds->_validate_message(JSON::XS::encode_json([$data]), "foo-user");
ok($result, "restricted user IS allowed to push when all constraints met");


# A few additional tests - make sure we check every update in the array. Any failure
# results in throwing away the whole message.
$data     = _gen_data("interface.metadata", "rtr.foo.edu", "xe-1/0/0", {"description" => "New description"});
$data2 = _gen_data("interface.metadata", "rtr.chic", "xe-1/0/0", {"description" => "New description"});
$result = $push_ds->_validate_message(JSON::XS::encode_json([$data, $data2]), "foo-user");
ok(! $result, "restricted user NOT allowed to push due to metadata patterns in multiple messages");
ok($push_ds->error() =~ /Metadata node = rtr.chic not allowed/, "got correct error message");


# Test whether a user with only database level constraints can push, ie no metadata restrictions
$data     = _gen_data("otherdb.metadata", "rtr.foo.edu", "xe-1/0/0", {"description" => "New description"});
$result = $push_ds->_validate_message(JSON::XS::encode_json([$data]), "foo-user");
ok($result, "restricted user IS allowed to push to database without metadata restrictions");

sub _gen_data {
    my $db           = shift;
    my $node         = shift;
    my $intf         = shift;
    my $extra_fields = shift || {};


    my $base = {
	"interval" =>  60,
	"meta" => {
	    "intf" => $intf,
	    "node" => $node
	},
	"time" => int(time),
	"type" => $db,
	"values" => {
	    "input" => 12345,
	    "output" => 67890
        }
    };

    foreach my $key (keys %$extra_fields){
	$base->{$key} = $extra_fields->{$key};
    }

    return $base;
}
