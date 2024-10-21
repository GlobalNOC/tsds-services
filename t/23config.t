use strict;
use warnings;

use Data::Dumper;
use FindBin;
use Test::More tests => 4;

use GRNOC::TSDS::Config;

my $config_file = "$FindBin::Bin/conf/config.xml";
my $log_file = "$FindBin::Bin/../conf/logging.conf";

GRNOC::Log->new(config => $log_file);


my $c = new GRNOC::TSDS::Config(
    config_file => $config_file,
);
ok($c->mongodb_user eq "tsds_rw");


my $d = new GRNOC::TSDS::Config();

ok(!defined $d->mongodb_user);

$ENV{MONGODB_USER} = "example";
ok($d->mongodb_user eq "example");

ok(!defined $d->mongodb_uri);
