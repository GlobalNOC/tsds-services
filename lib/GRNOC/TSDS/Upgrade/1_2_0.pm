package GRNOC::TSDS::Upgrade::1_2_0;

use strict;
use warnings;

# upgrade any prior version
use constant PREVIOUS_VERSION => undef;

sub upgrade {

    my ( $self, $upgrade ) = @_;

    ### UPGRADE CODE GOES HERE ###

    my $mongo = $upgrade->mongo;

    my @dbs = $mongo->database_names();

    foreach my $db_name ( @dbs ) {

        my $db = $mongo->get_database( $db_name );

        my @cols = $db->collection_names();

        my $has_metadata = grep( /metadata/, @cols );

        next if !$has_metadata;

        my $has_aggregate = grep( /aggregate/, @cols );

        next if !$has_aggregate;

        my $metadata_col = $db->get_collection( 'metadata' );
        my $agg_col = $db->get_collection( 'aggregate' );

        my @aggs = $agg_col->find( {} )->fields( {'_id' => 1 } )->all();

        foreach my $agg ( @aggs ) {

            my $agg_id = $agg->{'_id'};

            $agg_col->update( {'_id' => $agg_id}, {'$unset' => {'hist_res' => ''}} );

            my $metadata_values = $metadata_col->find( {} )->fields( {'values' => 1} )->next()->{'values'};

            next if !$metadata_values;

            my @metadata_values = keys( %$metadata_values );

            next if !@metadata_values;

            foreach my $metadata_value ( @metadata_values ) {

                my $hist_res = undef;
                my $min_width = undef;

                if ( $metadata_value eq 'input' || $metadata_value eq 'output' ) {

                    $hist_res = 0.1;
                    $min_width = 10000000;
                }

                elsif ( $metadata_value =~ /discard/ || $metadata_value =~ /error/ || $metadata_value =~ /Ucast/ ) {

                    $hist_res = 0.1;
                    $min_width = 10000;
                }

                elsif ( $metadata_value eq 'cpu' ) {

                    $hist_res = 1;
                    $min_width = 1;
                }

                $agg_col->update( {'_id' => $agg_id}, {'$set' => {"values.$metadata_value" => {'hist_res' => $hist_res,
                                                                                               'hist_min_width' => $min_width}}} );
            }
        }
    }

    ### END UPGRADE CODE ###

    return 1;
}

1;
