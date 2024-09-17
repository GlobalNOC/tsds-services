#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
#--------------------------------------------------------------------
#----- GRNOC TSDS Push GWS Library
#-----
#----- Copyright(C) 2013 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $LastChangedBy: $
#----- $LastChangedRevision: $
#----- $LastChangedDate: $
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/GWS/Push.pm $
#----- $Id: Push.pm 31432 2014-06-16 21:02:01Z charmadu $
#-----
#----- This module inherits the base GRNOC::TSDS::GWS class and
#----- provides all of the webservice methods to interact with the
#----- Push DataService.
#--------------------------------------------------------------------

package GRNOC::TSDS::GWS::InfluxDB;

use strict;
use warnings;

use GRNOC::TSDS::DataService::Push;
use GRNOC::TSDS::InfluxDB;
use GRNOC::TSDS::Cache;

use GRNOC::WebService::Method;
use GRNOC::WebService::Regex;

use Data::Dumper;
use JSON;

sub new {
    my $class = shift;
    my $args  = {
        @_
    };
    my $self = bless $args, $class;

    # get/store our data service
    $self->{push_ds} = new GRNOC::TSDS::DataService::Push(%$args);
    $self->{influxdb} = new GRNOC::TSDS::InfluxDB(%$args);
    $self->{cache} = new GRNOC::TSDS::Cache(%$args);

    return $self;
}

sub _add_influx_data {
    my ( $self, $args ) = @_;

    # my %processed = $self->process_args( $args );

    # $processed{'user'} = $ENV{'REMOTE_USER'};

    # Convert Line Protocol into traditional TSDS data structures
    my $data;
    eval {
        $data = $self->{influxdb}->parse($args->{data});
    };
    if ($@) {
        die $@;
        return;
    }

    foreach my $measurement (sort { $a->{'time'} <=> $b->{'time'} } @$data) {
        my $prev_measurement = $self->{cache}->get_prev_measurement_values($measurement);
        if (!defined $prev_measurement) {
            warn "Couldn't find previous values for $measurement->{type}.";
            # Because we enable to find previous values for this
            # measurement, cacluating rates for counters is impossible.
            # We instead set $prev_measurement to an empty hash and
            # continue, which allows non-counters to be recorded.
            $prev_measurement = {};
        }
        $self->{cache}->set_measurement_values($measurement);

        my $counters = $self->{cache}->get_data_type_counter_values($measurement->{type});
        foreach my $key (keys %{$measurement->{values}}) {
            # Ignore this block's logic if value is not a counter
            if (!$counters->{$key}) {
                next;
            }

            if (defined $prev_measurement->{$key}) {
                $measurement->{values}->{$key} = $measurement->{values}->{$key} - $prev_measurement->{$key};
            } else {
                # If a value is missing from a previous measurement, this might imply
                # tsds hasn't initialized the measurement document's 3D array for
                # the given value. We don't have to handle that though.
                $measurement->{values}->{$key} = undef;
            }
        }
    }

    $args->{data} = encode_json($data);
    my $results = $self->{push_ds}->add_data( %$args );
    if ( !$results ) {
        die $self->{push_ds}->error();
        return;
    }

    # $method->set_headers([{name => "-status", value => "204 OK"}]);
    return { results => $results };
}

1;
