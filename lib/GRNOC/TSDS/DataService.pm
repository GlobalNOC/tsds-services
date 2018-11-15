#--------------------------------------------------------------------
#----- GRNOC TSDS DataService Library
#-----
#----- Copyright(C) 2015 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/DataService.pm $
#----- $Id: DataService.pm 39919 2015-10-28 13:14:11Z mrmccrac $
#-----
#----- This is a base class that other DataService modules should
#----- inherit.  It provides common helper methods that all
#----- DataServices can/should use, as well as sets up the database
#----- handles needed.
#--------------------------------------------------------------------

package GRNOC::TSDS::DataService;

use strict;
use warnings;

use GRNOC::Log;
use GRNOC::Config;
use GRNOC::TSDS::MongoDB;

sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class    = $caller if ( !$class );

    # create object
    my $self = {'config_file'           => undef,
                'logging_config_file'   => undef,
                'error'                 => undef,
                @_};

    bless( $self, $class );

    # attempt to initialize
    $self->_init();

    return $self;
}

sub config {

    my ( $self, $config ) = @_;

    $self->{'config'} = $config if ( defined( $config ) );

    return $self->{'config'};
}

sub error {

    my ( $self, $error ) = @_;

    if ( defined( $error ) ) {

        $self->{'error'} = $error;
        log_error($error);
    }

    return $self->{'error'};
}

sub _init {

    my $self = shift;

    # parse & store config file
    my $config = GRNOC::Config->new( config_file => $self->{'config_file'},
                                     force_array => 0 );

    $self->config( $config );

}

sub query_ds {

    my ( $self, $query_ds ) = @_;

    $self->{'query_ds'} = $query_ds if ( defined( $query_ds ) );

    return $self->{'query_ds'};
}

sub metadata_ds {

    my ( $self, $metadata_ds ) = @_;

    $self->{'metadata_ds'} = $metadata_ds if ( defined( $metadata_ds ) );

    return $self->{'metadata_ds'};
}

sub parser {

    my ( $self, $parser ) = @_;

    $self->{'parser'} = $parser if ( defined( $parser ) );

    return $self->{'parser'};
}

sub mongo_ro {

    my ( $self, $mongo ) = @_;

    $self->{'mongo_ro'} = $mongo if ( defined( $mongo ) );

    if (! defined($self->{'mongo_ro'}) && $self->{'config_file'}){
	$self->{'mongo_ro'} = GRNOC::TSDS::MongoDB->new( config_file => $self->{'config_file'}, privilege => 'ro' );
    }

    return $self->{'mongo_ro'};
}

sub mongo_rw {

    my ( $self, $mongo ) = @_;

    $self->{'mongo_rw'} = $mongo if ( defined( $mongo ) );

    if (! defined($self->{'mongo_rw'}) && $self->{'config_file'}){
	$self->{'mongo_rw'} = GRNOC::TSDS::MongoDB->new( config_file => $self->{'config_file'}, privilege => 'rw' );
    }

    return $self->{'mongo_rw'};
}

sub mongo_root {

    my ( $self, $mongo ) = @_;

    $self->{'mongo_root'} = $mongo if ( defined( $mongo ) );

    if (! defined($self->{'mongo_root'}) && $self->{'config_file'}){
	$self->{'mongo_root'} = GRNOC::TSDS::MongoDB->new( config_file => $self->{'config_file'}, privilege => 'root' );
    }

    return $self->{'mongo_root'};
}

sub redislock {

    my ( $self, $redislock ) = @_;

    $self->{'redislock'} = $redislock if ( defined( $redislock ) );

    return $self->{'redislock'};
}

sub parse_int {
    my ($self, $int) = @_;

    if(defined($int)){
        $int = int($int);
    }

    return $int; 
}

sub format_find {
    my ($self, %args) = @_;
    my $find        = $args{'find'} || {};
    my $find_logic  = $args{'find_logic'} || '$and';
    my $field_logic = $args{'field_logic'} || '$or';
    my $field       = $args{'field'};
    my $values      = $args{'values'};

    if(!defined($values)){
        return $find;
    }

    if(!defined($find->{$find_logic})){
        $find->{$find_logic} = [];
    }

    my $formatted_values = { $field_logic => [] };
    foreach my $value (@$values){
        push(@{$formatted_values->{$field_logic}}, { $field => $value});
    }
    push(@{$find->{$find_logic}}, $formatted_values);

    return $find;
}

sub update_constraints_file {

    my ( $self, $constraints_file ) = @_;

    $self->parser->update_constraints_file($constraints_file);

}

1;
