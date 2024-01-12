#--------------------------------------------------------------------
#----- GRNOC TSDS Report GWS Library
#-----
#----- Copyright(C) 2013 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $LastChangedBy: $
#----- $LastChangedRevision: $
#----- $LastChangedDate: $
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/GWS/Forge.pm $
#----- $Id: Forge.pm 38652 2015-08-12 20:13:01Z prattadi $
#-----
#----- This module inherits the base GRNOC::TSDS::GWS class and
#----- provides all of the webservice methods to interact with the
#----- Report DataService.
#--------------------------------------------------------------------

package GRNOC::TSDS::GWS::Forge;

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';
use lib './venv/lib/perl5';

use base 'GRNOC::TSDS::GWS';

use GRNOC::TSDS::DataService::Report;

use GRNOC::WebService::Method;
use GRNOC::WebService::Regex;

use Data::Dumper;

sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    my $self = $class->SUPER::new( @_ );

    bless( $self, $class );

    # get/store our data service
    $self->report_ds( GRNOC::TSDS::DataService::Report->new( @_ ) );

    return $self;
}

sub _init_methods {

    my $self = shift;

    my $method;

    # define add_report method
    $method = GRNOC::WebService::Method->new( name          => 'add_report',
                                              description   => 'Add a new report',
                                              expires       => '-1d',
                                              callback      => sub { $self->_add_report( @_ ) } );

    $method->add_input_parameter( name          => 'name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the report' );

    $method->add_input_parameter( name          => 'type',
                                  pattern       => '^(portal|devel)$',
                                  required      => 0,
                                  default       => 'portal',
                                  multiple      => 0,
                                  description   => 'The type of report' );
    
    $method->add_input_parameter( name          => 'description',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The description of report' );
    
    $method->add_input_parameter( name          => 'default_timeframe',
                                  pattern       => $TEXT,
                                  default       => 86400,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The default timeframe of the report in seconds.' );

    $method->add_input_parameter( name          => 'template',
                                  pattern       => '^basic/basic.html$',
                                  default       => 'basic/basic.html',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The html template the report will use' );
    
    $method->add_input_parameter( name          => 'component_type',
                                  pattern       => '^dataexplorer_tree$',
                                  default       => 'dataexplorer_tree',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The type of component the default report component added should be' );

    $self->websvc()->register_method( $method );

    # add update_report_branch
    $method = GRNOC::WebService::Method->new( name          => 'update_report_branch',
                                              description   => 'Edit a branch of a report',
                                              expires       => '-1d',
                                              callback      => sub { $self->_update_report_branch( @_ ) } );

    $method->add_input_parameter( name          => 'report_name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the report the branch is contained in' );
    
    $method->add_input_parameter( name          => 'name',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The name you would like to rename the branch to' );
    
    $method->add_input_parameter( name          => 'component_index',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  default       => 0,
                                  multiple      => 0,
                                  description   => 'The index of the component containing the branch you wish to edit' );

    $method->add_input_parameter( name          => 'key',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  default       => 0,
                                  multiple      => 0,
                                  description   => 'The key of the branch you wish to edit' );

    $method->add_input_parameter( name          => 'query',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'A json blob containing the query the branch will execute when activated' );
    
    $method->add_input_parameter( name          => 'chart_query',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'A json blob containing the query used by charts when the branch is activated' );
    
    $method->add_input_parameter( name          => 'header_query',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'A json blob containing the query used by the header when the branch is activated' );
    
    $method->add_input_parameter( name          => 'header_title',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'A json blob containing the title used by the header when the branch is activated' );
    
    $method->add_input_parameter( name          => 'header_subtitle',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'A json blob containing the subtitle used by the header when the branch is activated' );

    $self->websvc()->register_method( $method );

    # add add_report_branch
    $method = GRNOC::WebService::Method->new( name          => 'add_report_branch',
                                              description   => 'Add a branch to a report',
                                              expires       => '-1d',
                                              callback      => sub { $self->_add_report_branch( @_ ) } );

    $method->add_input_parameter( name          => 'report_name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the report the branch should be added to' );
    
    $method->add_input_parameter( name          => 'name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the branch' );
    
    $method->add_input_parameter( name          => 'type',
                                  pattern       => '^(folder|expandable_list|chart_list)$',
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The type of branch to be added' );
    
    $method->add_input_parameter( name          => 'key',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The key of the branch this branch should be added above or below. If the location above is chosen with no key, the new branch becomes the root. If the same situation with below chosen as the location happens, the branch is added at the top level' );

    $method->add_input_parameter( name          => 'query',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The query to be executed when a branch is activated. Required if branch is not a folder.' );

    $method->add_input_parameter( name          => 'chart_query',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The chart query to be executed when a branch is activated.' );

    $method->add_input_parameter( name          => 'location',
                                  pattern       => '^(above|below)$',
                                  default       => 'below',
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'Whether you want the branch added above or below the specified key' );
    
    $method->add_input_parameter( name          => 'component_index',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  default       => 0,
                                  multiple      => 0,
                                  description   => 'The index of the component this branch should be added to' );

    $self->websvc()->register_method( $method );

    # add delete_reports method
    $method = GRNOC::WebService::Method->new( name          => 'delete_reports',
                                              description   => 'Deletes specified reports',
                                              expires       => '-1d',
                                              callback      => sub { $self->_delete_reports( @_ ) } );

    $method->add_input_parameter( name          => 'name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 1,
                                  description   => 'The names of the reports to delete' );
    
    $self->websvc()->register_method( $method );

    # add update_reports method
    $method = GRNOC::WebService::Method->new( name          => 'update_reports',
                                              description   => 'Updates specified reports',
                                              expires       => '-1d',
                                              callback      => sub { $self->_update_reports( @_ ) } );

    $method->add_input_parameter( name          => 'name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 1,
                                  description   => 'The names of the reports to update' );
    
    $method->add_input_parameter( name          => 'new_name',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The name you wish to change your report to' );

    $method->add_input_parameter( name          => 'description',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The description you wish to change your report to' );

    $method->add_input_parameter( name          => 'default_timeframe',
                                  pattern       => $TEXT,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The timeframe you wish to change your report to' );

    $self->websvc()->register_method( $method );

    # add update_reports method
    $method = GRNOC::WebService::Method->new( name          => 'update_report_component',
                                              description   => 'Updates specified report component',
                                              expires       => '-1d',
                                              callback      => sub { $self->_update_report_component( @_ ) } );

    $method->add_input_parameter( name          => 'report_name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the reports to update' );

    $method->add_input_parameter( name          => 'component_index',
                                  pattern       => '^(\d+)$',
                                  required      => 0,
                                  default       => 0,
                                  multiple      => 0,
                                  description   => 'The index of the component to edit' );
                                          
    $method->add_input_parameter( name          => 'header_title',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The header title you wish to change your report to' );

    $method->add_input_parameter( name          => 'header_subtitle',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The header subtitle you wish to change your report to' );
    
    $self->websvc()->register_method( $method );

    # add delete_report_branch method
    $method = GRNOC::WebService::Method->new( name          => 'delete_report_branch',
                                              description   => 'Deletes specified report branch',
                                              expires       => '-1d',
                                              callback      => sub { $self->_delete_report_branch( @_ ) } );

    $method->add_input_parameter( name          => 'report_name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the report you wish to delete a branch from' );
    
    $method->add_input_parameter( name          => 'component_index',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  default       => 0,
                                  description   => 'The index of the component that contains the branch you wish to delete' );

    $method->add_input_parameter( name          => 'key',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The key of the branch you wish to delete' );
    
                              
    $self->websvc()->register_method( $method );
    
    # add move_report_branch method
    $method = GRNOC::WebService::Method->new( name          => 'move_report_branch',
                                              description   => 'Moves specified report branch below another branch',
                                              expires       => '-1d',
                                              callback      => sub { $self->_move_report_branch( @_ ) } );
    
    $method->add_input_parameter( name          => 'report_name',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The name of the report you wish to move a branch withing' );
    
    $method->add_input_parameter( name          => 'component_index',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  default       => 0,
                                  description   => 'The index of the component you wish to move a branch within' );
    
    $method->add_input_parameter( name          => 'key',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The key of the branch you wish to move' );

    $method->add_input_parameter( name          => 'below_key',
                                  pattern       => $NAME_ID,
                                  required      => 1,
                                  multiple      => 0,
                                  description   => 'The key of the branch you wish to move below' );

    $self->websvc()->register_method( $method );

    # add upload_header_image method
    $method = GRNOC::WebService::Method->new( name          => 'upload_header_image',
                                              description   => 'Uploads an image.',
                                              expires       => '-1d',
                                              callback      => sub { $self->_upload_header_image( @_ ) } );

    # register the optional 'image' input param to the upload_header_image() method.
    $method->add_input_parameter( name => 'image',
                                  pattern => '.*',
                                  required => 1,
                                  multiple => 0,
                                  description => 'The image to upload.',
                                  attachment => 1 );

    $method->add_input_parameter( name        => 'report_name',
                                  pattern     => $NAME_ID,
                                  required    => 1,
                                  description => 'The name of the report.' );
    
    $method->add_input_parameter( name          => 'key',
                                  pattern       => $NAME_ID,
                                  required      => 0,
                                  multiple      => 0,
                                  description   => 'The key of the branch you wish to add an image to, if not defined adds image to the component header' );

    $method->add_input_parameter( name        => 'name',
                                  pattern     => $NAME_ID,
                                  required    => 1,
                                  description => 'The image name.' );

    $method->add_input_parameter( name         => 'component_index',
                                  pattern      => '^(\d+)$',
                                  required     => 0,
                                  default      => 0,
                                  description  => 'The HTML to render as an image.' );


    # register the get_measurement_types() method
    $self->websvc()->register_method( $method );


}

# callbacks
sub _add_report {

    my ( $self, $method, $args ) = @_;
  
    my $results = $self->report_ds()->add_report( 
        remote_user => $ENV{'REMOTE_USER'},
        $self->process_args( $args )
    );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->report_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _delete_reports {

    my ( $self, $method, $args ) = @_;

    my $results = $self->report_ds()->delete_reports( 
        remote_user => $ENV{'REMOTE_USER'},
        $self->process_args( $args )
    );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->report_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _update_reports {

    my ( $self, $method, $args ) = @_;

    my $results = $self->report_ds()->update_reports( 
        remote_user => $ENV{'REMOTE_USER'},
        $self->process_args( $args )
    );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->report_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _update_report_branch {

    my ( $self, $method, $args ) = @_;

    my $results = $self->report_ds()->update_report_branch( 
        remote_user => $ENV{'REMOTE_USER'},
        $self->process_args( $args )
    );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->report_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _add_report_branch {

    my ( $self, $method, $args ) = @_;

    my $results = $self->report_ds()->add_report_branch( 
        remote_user => $ENV{'REMOTE_USER'},
        $self->process_args( $args )
    );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->report_ds()->error() );
        return;
    }

    return {
        results => $results,
    };
}

sub _delete_report_branch {

    my ( $self, $method, $args ) = @_;

    my $results = $self->report_ds()->delete_report_branch( 
        remote_user => $ENV{'REMOTE_USER'},
        $self->process_args( $args )
    );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->report_ds()->error() );
        return;
    }

    return {
        results => $results,
    };

}

sub _move_report_branch {
    my ( $self, $method, $args ) = @_;

    my $results = $self->report_ds()->move_report_branch( 
        remote_user => $ENV{'REMOTE_USER'},
        $self->process_args( $args )
    );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->report_ds()->error() );
        return;
    }

    return {
        results => $results,
    };

}

sub _update_report_component {
    my ( $self, $method, $args ) = @_;

    my $results = $self->report_ds()->update_report_component( 
        remote_user => $ENV{'REMOTE_USER'},
        $self->process_args( $args )
    );

    # handle error
    if ( !$results ) {

        $method->set_error( $self->report_ds()->error() );
        return;
    }

    return {
        results => $results,
    };

}

sub _upload_header_image {

    my ( $self, $method, $args ) = @_;

    my $results = $self->report_ds()->upload_header_image( 
        remote_user => $ENV{'REMOTE_USER'},
        mime_type   => $args->{'image'}{'mime_type'},
        $self->process_args( $args ) 
    );

    # handle error
    if ( !$results ) {
        $method->set_error( $self->report_ds()->error() );
        return;
    }

    return {'results' => $results};
}

sub update_constraints_file {

    my ( $self, $constraints_file ) = @_;

    $self->report_ds->update_constraints_file($constraints_file);

}

1;

