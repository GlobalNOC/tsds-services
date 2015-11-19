#!/usr/bin/perl

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;
use GRNOC::TSDS::Parser; # including b/c Marpa will be a PITA otherwise
use GRNOC::TSDS::SearchIndexer;

### constants ###

use constant DEFAULT_CONFIG_FILE   => '/etc/grnoc/tsds/services/config.xml';
use constant DEFAULT_LOGGING_FILE  => '/etc/grnoc/tsds/services/logging.conf';
use constant DEFAULT_TEMPLATE_DIR  => '/etc/grnoc/tsds/services/sphinx_templates/';

### command line options ###

my $help;
my $debug;
my $quiet;
my $pretend;
my $database;

my $config = DEFAULT_CONFIG_FILE;
my $logging = DEFAULT_LOGGING_FILE;
my $template_dir = DEFAULT_TEMPLATE_DIR;
my $last_updated_offset = 0;

# parse options from command line
GetOptions(
    "quiet"                 => \$quiet,
    "debug"                 => \$debug,
    "help|h|?"              => \$help,
    "pretend"               => \$pretend,
    "config=s"              => \$config,
    "logging=s"             => \$logging,
    "database=s"            => \$database,
    "last-updated-offset|u=i" => \$last_updated_offset,
    "template-dir=s"        => \$template_dir );

# did they ask for help?
usage() if $help;

# instatiate logging
GRNOC::Log->new( config => $logging );

# instantiation retention agent object
my $agent = GRNOC::TSDS::SearchIndexer->new(
    quiet                => $quiet,
    database             => $database,
    pretend              => $pretend,
    config_file          => $config,
    last_updated_offset  => $last_updated_offset,
    sphinx_templates_dir => $template_dir );

# run with options provided
$agent->index_metadata();

### private methods ###
sub usage {

    print "Usage: $0\n".
        "  [--config <file path>]\n".
        "  [--logging <file path>]\n".
        "  [--template-dir <dir path>]\n".
        "  [--database <database>]\n".
        "  [--aggregate <name>]\n".
        "  [--query <query>]\n".
        "  [--last-updated-offset <num minutes>]\n".
        "  [--quiet]\n".
        "  [--pretend]\n".
        "  [--help]\n";

    exit( 1 );
}
