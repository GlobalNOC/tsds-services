#--------------------------------------------------------------------
#----- GRNOC TSDS GlobalNOC Web Service (GWS) Library
#-----
#----- Copyright(C) 2014 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $LastChangedBy: $
#----- $LastChangedRevision: $
#----- $LastChangedDate: $
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/GWS.pm $
#----- $Id: GWS.pm 36955 2015-04-30 21:12:27Z mj82 $
#-----
#----- This is a base class that other GWS modules should
#----- inherit.  It provides common helper methods that all GWS
#----- modules can/should use.
#--------------------------------------------------------------------

package GRNOC::TSDS::GWS;

use strict;
use warnings;

use GRNOC::Config;
use GRNOC::WebService::Dispatcher;

use HTML::Parser;
use Data::Dumper;

use constant MAX_UPLOAD_SIZE => 104_857_600;

sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    my $self = {
        config_file => undef,
        @_
    };

    bless( $self, $class );

    $self->_init();

    return $self;
}

sub config {

    my ( $self, $config ) = @_;

    $self->{'config'} = $config if ( defined( $config ) );

    return $self->{'config'};
}

sub _init {

    my $self = shift;

    $self->_init_config();
    $self->_init_websvc();

    $self->_init_methods();
}

sub _init_config {

    my $self = shift;

    my $config = GRNOC::Config->new( config_file => $self->{'config_file'},
                                     force_array => 0 );

    $self->config( $config );
}

sub _init_websvc {

    my $self = shift;

    my $config = $self->config();

    $config->{'force_array'} = 1;
    my $proxy_users = $config->get( '/config/proxy-users/username' );
    $config->{'force_array'} = 0;

    # create websvc dispatcher object
    my $websvc = GRNOC::WebService::Dispatcher->new( allowed_proxy_users => $proxy_users,
                                                     max_post_size       => MAX_UPLOAD_SIZE );

    # add the input validator which will reject any input that contains HTML
    $websvc->add_default_input_validator( name          => 'disallow_html',
                                          description   => 'This default input validator will invalidate any input that contains HTML.',
                                          callback      => sub { $self->_disallow_html( @_ ); } );

    $self->websvc( $websvc );

}

sub _init_methods {

}

sub _disallow_html {

    my ( $self, $method, $input ) = @_;

    my $parser = HTML::Parser->new();

    my $contains_html = 0;

    $parser->handler( start => sub { $contains_html = 1 }, 'tagname' );

    $parser->parse( $input );
    $parser->eof();

    return !$contains_html;
}

sub websvc {

    my ( $self, $websvc ) = @_;

    $self->{'websvc'} = $websvc if ( defined( $websvc ) );

    return $self->{'websvc'};
}

sub handle_request {

    my $self = shift;

    $self->{'websvc'}->handle_request( $self );
}

sub atlas_ds {

    my ( $self, $atlas_ds ) = @_;

    $self->{'atlas_ds'} = $atlas_ds if ( defined( $atlas_ds ) );

    return $self->{'atlas_ds'};
}

sub query_ds {

    my ( $self, $query_ds ) = @_;

    $self->{'query_ds'} = $query_ds if ( defined( $query_ds ) );

    return $self->{'query_ds'};
}

sub worldview_ds {

    my ( $self, $worldview_ds ) = @_;

    $self->{'worldview_ds'} = $worldview_ds if ( defined( $worldview_ds ) );

    return $self->{'worldview_ds'};
}

sub metadata_ds {

    my ( $self, $metadata_ds ) = @_;

    $self->{'metadata_ds'} = $metadata_ds if ( defined( $metadata_ds ) );

    return $self->{'metadata_ds'};
}

sub aggregation_ds {

    my ( $self, $aggregation_ds ) = @_;

    $self->{'aggregation_ds'} = $aggregation_ds if ( defined( $aggregation_ds ) );

    return $self->{'aggregation_ds'};
}

sub image_ds {

    my ( $self, $image_ds ) = @_;

    $self->{'image_ds'} = $image_ds if ( defined( $image_ds ) );

    return $self->{'image_ds'};
}

sub push_ds {

    my ( $self, $push_ds ) = @_;

    $self->{'push_ds'} = $push_ds if ( defined( $push_ds ) );

    return $self->{'push_ds'};
}

sub report_ds {

    my ( $self, $report_ds ) = @_;

    $self->{'report_ds'} = $report_ds if ( defined( $report_ds ) );

    return $self->{'report_ds'};
}

sub search_ds {

    my ( $self, $search_ds ) = @_;

    $self->{'search_ds'} = $search_ds if ( defined( $search_ds ) );

    return $self->{'search_ds'};
}

sub process_args {

    my ( $self, $args ) = @_;

    my %results;

    my @names = keys( %$args );

    foreach my $name ( @names ) {

        if ( $args->{$name}{'is_set'} ) {

            $results{$name} = $args->{$name}{'value'};
        }
    }

    return %results;
}

1;
