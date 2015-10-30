#!/usr/bin/perl

use strict;
use warnings;

use GRNOC::TSDS::Expire;
use JSON::XS;

use Getopt::Long;
use Data::Dumper;

### constants ###

use constant DEFAULT_CONFIG_FILE => '/etc/grnoc/tsds/services/config.xml';
use constant DEFAULT_LOGGING_FILE => '/etc/grnoc/tsds/services/logging.conf';
use constant DEFAULT_LOCK_DIR => '/var/run/grnoc/tsds/services/';

### command line options ###

my $help;
my $debug;
my $config = DEFAULT_CONFIG_FILE;
my $logging = DEFAULT_LOGGING_FILE;
my $database;
my $expire;
my $query;
my $start;
my $end;
my $pretend;

# parse options from command line
GetOptions( "help|h|?" => \$help,
            "debug" => \$debug,
            "config=s" => \$config,
            "logging=s" => \$logging,
            "database=s" => \$database,
            "expire=s" => \$expire,
            "query=s" => \$query,
            "start=i" => \$start,
            "end=i" => \$end,
            "pretend" => \$pretend );

# did they ask for help?
usage() if ( $help );

# did they specify a query?
if ( defined( $query ) ) {

    # json decode it
    my $json = JSON::XS->new();

    $query = $json->decode( $query );
}

my $expirator = GRNOC::TSDS::Expire->new( config_file => $config,
                                          logging_file => $logging,
                                          database => $database,
                                          expire => $expire,
                                          query => $query,
                                          start => $start,
                                          end => $end,
                                          pretend => $pretend );

# run with options provided
$expirator->expire_data();

### private methods ###

sub usage {

    print "Usage: $0 [--config <file path>] [--logging <file path>] [--database <database>] [--expire <name>] [--query <query>] [--start <epoch>] [--end <epoch>] [--pretend] [--help]\n";
    exit( 1 );
}
