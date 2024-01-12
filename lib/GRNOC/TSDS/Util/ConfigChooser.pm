#!/usr/bin/perl

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';
use lib './venv/lib/perl5';

package GRNOC::TSDS::Util::ConfigChooser;

use GRNOC::Config;

use constant DEFAULT_MAPPING_FILE => '/etc/grnoc/tsds/services/mappings.xml';
use constant DEFAULT_CONFIG_FILE  => '/etc/grnoc/tsds/services/config.xml';
use constant DEFAULT_LOGGING_FILE => '/etc/grnoc/tsds/services/logging.conf';

sub get_location {
  my $self = shift;
  my $url = shift;
  my $mappings_file = shift;

  my $location = {
                     config_location  => undef,
                     logging_location => DEFAULT_LOGGING_FILE
                 };

  if (!defined($mappings_file)) {
      $mappings_file = DEFAULT_MAPPING_FILE;
  }

  #if mappings file is not found, use default file locations
  unless (-e $mappings_file) {
     return $location;
  }

  my $config = GRNOC::Config->new( config_file => $mappings_file, force_array => 1 );

  my $entries = $config->get( '/mappings/map' );


  foreach my $entry ( @$entries ) {
     my $regexp = $entry->{'regexp'};
     if ( $url =~ /$regexp/ ) {
        my $config_location = $entry->{'config_location'};
        my $logging_location = $entry->{'logging_location'};

        $location->{'config_location'} = $config_location if (defined($config_location) and (-e $config_location));
        $location->{'logging_location'} = $logging_location if (defined($logging_location) and (-e $logging_location));

        last;
     }
  }

  return $location;
}

1;

