#--------------------------------------------------------------------
#----- GRNOC TSDS Report DataService Library
#-----
#----- Copyright(C) 2013 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $LastChangedBy: $
#----- $LastChangedRevision: $
#----- $LastChangedDate: $
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/DataService/Report.pm $
#----- $Id: Report.pm 38924 2015-09-01 19:43:29Z prattadi $
#-----
#----- This module inherits the base GRNOC::TSDS::DataService class
#--------------------------------------------------------------------

package GRNOC::TSDS::DataService::Report;

use strict;
use warnings;

use base 'GRNOC::TSDS::DataService';

use GRNOC::TSDS::DataService::MetaData;
use GRNOC::TSDS::MongoDB;
use MIME::Base64;
use Image::Magick;
use Data::Dumper;
use JSON qw( decode_json );
use Storable qw(dclone);
use Template;
use URI;

use constant GLOBAL_VIEW_KEY => '/tsds/services';

# this will hold the only actual reference to this object
my $singleton;

sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    # if we've created this object (singleton) before, just return it
    return $singleton if ( defined( $singleton ) );

    my $self = $class->SUPER::new( @_ );

    bless( $self, $class );

    # store our newly created object as the singleton
    $singleton = $self;

    # get/store the mongodb handle
    $self->mongo_rw( GRNOC::TSDS::MongoDB->new( @_, privilege => 'rw' ) );
    $self->mongo_ro( GRNOC::TSDS::MongoDB->new( @_, privilege => 'ro' ) );

    # store the other dataservices
    $self->metadata( GRNOC::TSDS::DataService::MetaData->new( @_ ) );

    my $config = GRNOC::Config->new( config_file => $self->{'config_file'}, force_array => 0);
    $self->{'config'} = $config;
    $self->{'proxy_users'} = $config->get('/config/proxy-users/username') || [];

    # Make sure it's an array, we set force_array to 0 above but we always
    # assume that the proxy users are in array form
    $self->{'proxy_users'} = [$self->{'proxy_users'}] if (! ref $self->{'proxy_users'});

    return $self;
}

sub update_constraints_file {
    my ( $self, $constraints_file ) = @_;
    
    if (defined($constraints_file)) {
        my $config = GRNOC::Config->new( config_file => $constraints_file, force_array => 0);
        $self->{'global_uri'} = $config->get( '/config/global-access/@uri' );
    }
    else {
        # no constraints file in mapping file, consider it has gloabl access view
        $self->{'global_uri'} = GLOBAL_VIEW_KEY;
    }
}

sub get_reports {

    my ( $self, %args ) = @_;

    my $name = $args{'name'};
    my $type = $args{'type'};
    my $order_by = $args{'order_by'};
    my $order = $args{'order'};
    my $limit = $args{'limit'};
    my $offset = $args{'offset'};


    my $sort = {};
    $sort->{'order'} = $order if $order;
    $sort->{'order_by'} = $order_by if $order_by;
    $sort->{'offset'} = $offset if $offset;
    $sort->{'limit'} = $limit if $limit;

    my $find = $self->format_find(
        field  => 'name',
        values => $name
    ); 
    $find = $self->format_find(
        field  => 'type',
        values => $type,
        find   => $find
    ); 

    my $results = $self->_get_reports($find, $sort);
    if ( !$results ) {
        $self->error( "Error getting reports" );
        return;
    }

    return $results;
}

sub add_report {
    my ( $self, %args ) = @_;

    my $name                = $args{'name'};
    my $type                = $args{'type'} || 'portal';
    my $description         = $args{'description'};
    my $default_timeframe   = $args{'default_timeframe'} || "86400";
    my $template            = $args{'template'} || "basic/basic.html";
    my $component_type      = $args{'component_type'} || "dataexplorer_tree";

    # check that user is defined and not a proxy user 
    $self->_valid_user( $args{'remote_user'} ) || return;
   
    # ensure there isn't already a report by this name
    my $report_details = $self->_get_report_details( name => $name );
    if(@$report_details != 0){
        $self->error( "A report named, $name, already exists." );
        return;
    }

    my @timeframe = split(',', $default_timeframe);
    @timeframe = map {$_ + 0} @timeframe;

    my $constraint_key = $self->_get_constraint_key();

    my $report = {
        name => $name,
        type => $type,
        description => $description, 
        default_timeframe => \@timeframe,
        template          => $template,
        constraint_key    => $constraint_key,
        component         => [{
            type          => $component_type,
            branch        => [],
            header        => {}
        }]
    };
    
    return $self->_add_report(report => $report);
}

sub delete_reports {
    my ( $self, %args ) = @_;
    my $name = $args{'name'};
   
    # check that user is defined and not a proxy user 
    $self->_valid_user( $args{'remote_user'} ) || return;

    # sanity checks
    if(!defined($name)){
        $self->error( "You must pass in a report name." );
        return;
    } 

    my $find = $self->format_find( 
        field  => 'name',
        values => $name
    );

    return $self->_delete_reports($find);
}

sub update_reports {
    my ( $self, %args ) = @_;

    my $name                = $args{'name'};
    my $new_name            = $args{'new_name'};
    my $description         = $args{'description'};
    my $default_timeframe   = $args{'default_timeframe'};

    # check that user is defined and not a proxy user 
    $self->_valid_user( $args{'remote_user'} ) || return;

    # sanity checks
    if(!defined($name)){
        $self->error( "You must pass in the name of the report you wish to edit." );
        return;
    }
    if(@$name > 1 && defined($new_name)){
        $self->error( "You can not update multiple reports and pass in the new_name parameter." );
        return;
    }

    # ensure there isn't already a report by this new name
    if (defined($new_name)) {
        if (@$name > 1) {
            $self->error( "You can not update multiple reports and pass in the new_name parameter." );
            return;
        }

        # ensure there isn't already a report by this new name
        my $report_details = $self->_get_report_details( name => $new_name );
        if (@$report_details != 0){
            $self->error( "A report named, $new_name, already exists." );
            return;
        }
    }

    #build find 
    my $find = $self->format_find(
        field  => 'name',
        values => $name
    );

    # build set 
    my $set = {};
    if(defined($new_name)){
        $set->{'name'} = $new_name;
    }
    if(defined($description)){
        $set->{'description'} = $description;
    }
    if(defined($default_timeframe)){
        my @timeframe = split(',', $default_timeframe);
        @timeframe = map {$_ + 0} @timeframe;
        $set->{'default_timeframe'} = \@timeframe;
    }

    return $self->_update_reports( $find, $set );
}

sub update_report_component {
    my ($self, %args) = @_;
    my $report_name       = $args{'report_name'};
    my $component_index   = $args{'component_index'} || 0;
    my $header_title      = $args{'header_title'};
    my $header_subtitle   = $args{'header_subtitle'};
    my $header_image      = $args{'header_image'};
    my $header_image_name = $args{'header_image_name'};

    # check that user is defined and not a proxy user 
    $self->_valid_user( $args{'remote_user'} ) || return;

    # sanity checks
    if(!defined($report_name)){
        $self->error( "You must pass in the name of the report you wish to edit." );
        return;
    }

    # get this reports' component structure
    my $struct = $self->_get_components(
        report_name => $report_name,
        component_index => $component_index
    ) || return;
    my $component  = $struct->{'component'};
    my $components = $struct->{'components'};

    if(defined($header_title)){
        $component->{'header'}{'title'} = $header_title;
    }
    if(defined($header_subtitle)){
        $component->{'header'}{'subtitle'} = $header_subtitle;
    }
    if(defined($header_image)){
        $component->{'header'}{'image'}      = encode_base64( $header_image, '' );
        $component->{'header'}{'image_name'} = $header_image_name;
    }

    # now perform the actual edit
    $self->_update_report_components( report_name => $report_name, components => $components ) || return;


    return { component_index => $component_index };

}

sub delete_report_branch {
    my ($self, %args) = @_;
    my $key             = $args{'key'};
    my $report_name     = $args{'report_name'};
    my $component_index = $args{'component_index'} || 0;
    
    # check that user is defined and not a proxy user 
    $self->_valid_user( $args{'remote_user'} ) || return;
   
    # sanity checks 
    if(!defined($key)){
        $self->error( "You must pass in the key of the branch you wish to delete." );
        return;
    }
    if(!defined($report_name)){
        $self->error( "You must pass in the name of the report you wish to delete a branch from." );
        return;
    }

    # get this reports' component structure
    my $struct = $self->_get_components(
        report_name => $report_name,
        component_index => $component_index
    ) || return;
    my $component  = $struct->{'component'};
    my $components = $struct->{'components'};

    # get this reports' entire branch structure at the specified component_index
    my $branches = $component->{'branch'};
    if(!$branches){
        $self->error( "The report does not have a branch structure" );
        return;
    }

    # remove the branch with the passed in key 
    $self->_replace_branch(
        key => $key,
        branches => $branches
    ) || return;

    # now perform the actual edit
    $self->_update_report_components( report_name => $report_name, components => $components ) || return;

    return { key => $key };
}

sub add_report_branch {
    my ($self, %args) = @_;
    my $name               = $args{'name'};
    my $report_name        = $args{'report_name'};
    my $type               = $args{'type'};
    my $key                = $args{'key'};
    my $query              = $args{'query'};
    my $chart_query_params = $args{'chart_query'};
    my $location           = $args{'location'} || 'below';
    my $component_index    = $args{'component_index'} || 0;
    
    # check that user is defined and not a proxy user 
    $self->_valid_user( $args{'remote_user'} ) || return;

    # sanity checks
    if(!defined($name)){
        $self->error( "You must pass in a name for the branch." );
        return;
    }
    if(!defined($report_name)){
        $self->error( "You must pass in the branches' report_name." );
        return;
    }
    if(!defined($type)){
        $self->error( "You must pass in a type for the branch." );
        return;
    }
    if(!defined($query) && ($type ne 'folder') ){
        $self->error( "You must pass in a query structure for non-folder branch types." );
        return;
    }
    if($location ne 'above' && $location ne 'below'){
        $self->error( "Location can only equal above or below." );
        return;
    }
    if($type ne 'folder' && $type ne 'expandable_list' && $type ne 'chart_list'){
        $self->error( "Allowed types are folder, expandable_list, and chart_list." );
        return;
    }

    #initialize branch point
    my $branch = {
        name   => $name,
        type   => $type,
        branch => []
    };

    # if type isn't folder we have to add some query related foo
    if($type ne 'folder'){
        my $query = $self->_parse_json( json => $query, array => 1 ) || return;
        my $chart_query_params  = $self->_parse_json( json => $chart_query_params, array => 1 ) || return;

        $branch->{'query'} = $query;
        
        # for the time being make assumptions about chart_query stucture
        # branch query
        my $chart_query = $self->_get_chart_query( query => $query->[0], chart_query => $chart_query_params->[0] ) || return;
        $branch->{'chart'}{'query'} = $chart_query;

        # also make assumptions about header query
        my $header_query = $self->_get_header_query( query => $query->[0] ) || return;
        $branch->{'header'}{'query'} = $header_query;
    }
    

    # get this reports' component structure
    my $struct = $self->_get_components( 
        report_name => $report_name,
        component_index => $component_index
    ) || return;
    my $component  = $struct->{'component'};
    my $components = $struct->{'components'};

    # insert the branch into our branch structure
    my $new_key = $self->_insert_report_branch(
        component => $component,
        branch    => $branch,
        location  => $location,
        key       => $key
    );
    return if(!defined($new_key));

    # now perform the actual edit
    $self->_update_report_components( report_name => $report_name, components => $components ) || return;
    #return $components;
    return { key => $new_key };
}

# this function helps to insert a branch at a specific location within a branch structure
sub _insert_report_branch {
    my ($self, %args) = @_;
    my $branch    = $args{'branch'};
    my $component = $args{'component'};
    my $location  = $args{'location'};
    my $key       = $args{'key'};
   
    my $new_key; 

    # get this reports' entire branch structure
    my $branches = $component->{'branch'};
    if(!$branches){
        $self->error( "The report does not have a branch structure" );
        return;
    }

    # now determine where in the branch structure our new branch should be added
    if($location eq 'above') {
        # only allow folders to be inserted above branch structures
        if($branch->{'type'} ne 'folder'){
            $self->error( "You can not insert anything besides a branch type of folder above an existing branch" );
            return;
        }
        # if key is not defined put the whole branch structure underneath our new folder
        if(!defined($key)){
            # clone the current branch structure
            my $below_branches = dclone($branches);
            # delte the current branch structure from our component
            delete $component->{'branch'};
            # now add our new folder at the top of the structure
            $component->{'branch'} = [ $branch ];
            # finally push our copied branches below our new folder
            foreach my $below_branch (@$below_branches){
                push(@{$component->{'branch'}[0]{'branch'}}, $below_branch);
            }
            # our new key will be zero since it's now the top most branch
            $new_key = '0';
        }
        else {
            # otherwise retrieve the branch we want to insert above
            my $insertion_point_branch = $self->_get_branch(
                key => $key,
                branches => $branches
            ) || return;

            # our new key should be the key of our insertion point
            $new_key = $insertion_point_branch->{'key'};

            # clone that branch 
            my $below_branch = dclone($insertion_point_branch);

            # overwrite that branch reference with our new folder
            $self->_replace_branch(
                key => $key,
                branch => $branch,
                branches => $branches
            ) || return;

            # finally push our copied branch below our new folder
            push(@{$branch->{'branch'}}, $below_branch);
        }
    }else {
        # if no key was specified add this branch to the top of the branch structure
        if(!defined($key)){
            #our new key should be the branch length since we are now at the top of the structure
            $new_key = @$branches;
            push(@$branches, $branch);
        }
        # otherwise retrieve the branch we want to insert below
        else {
            my $insertion_point_branch = $self->_get_branch(
                key => $key,
                branches => $branches
            ) || return;
            # don't allow insertions below anything other than a folder
            if($insertion_point_branch->{'type'} ne 'folder'){
                $self->error( "You can only insert a branch below a folder" );
                return;
            }
            # our new key should use the key of our insertion point as a prefix
            $new_key = $insertion_point_branch->{'key'};
            # this append the branch length + 1 to get our entire key
            $new_key .= '-'.@{$insertion_point_branch->{'branch'}};

            push(@{$insertion_point_branch->{'branch'}}, $branch);
        }
    }
    return $new_key;
}

sub move_report_branch {
    my ($self, %args) = @_;
    my $key             = $args{'key'};
    my $report_name     = $args{'report_name'};
    my $component_index = $args{'component_index'} || 0;
    my $below_key       = $args{'below_key'};
    
    # check that user is defined and not a proxy user 
    $self->_valid_user( $args{'remote_user'} ) || return;

    #sanity checks
    if(!defined($key)){
        $self->error( "You must pass in the key of the branch you wish to move." );
        return;
    }
    if(!defined($below_key)){
        $self->error( "You must pass in the key of the branch you to move the branch below." );
        return;
    }
    if(!defined($report_name)){
        $self->error( "You must pass in the name of the report you wish to move a branch in." );
        return;
    }
    if($key eq $below_key){
        $self->error( "You cannot move a branch below itself." );
        return;
    }
    if($below_key =~ /^$key.+/){
        $self->error( "You cannot move a branch into it's own hierarchy." );
        return;
    }

    # get this reports' component structure
    my $struct = $self->_get_components(
        report_name => $report_name,
        component_index => $component_index
    ) || return;
    my $component  = $struct->{'component'};
    my $components = $struct->{'components'};

    # get this reports' entire branch structure at the specified component_index
    my $branches = $component->{'branch'};
    if(!$branches){
        $self->error( "The report does not have a branch structure" );
        return;
    }

    # make a copy of the branch we are moving
    my $branch_ref = $self->_get_branch(
        key => $key,
        branches => $branches
    ) || return;
    my $branch = dclone($branch_ref);

    # get the branch we are moving our branch below
    my $below_branch_ref = $self->_get_branch(
        key => $below_key,
        branches => $branches
    ) || return;

    # if below branch is not a folder don't allow the user to move somthing below it
    if($below_branch_ref->{'type'} ne 'folder'){
        $self->error( "You can only move branches below folders." );
        return;
    }

    # push our branch copy below the below_branch
    push(@{$below_branch_ref->{'branch'}}, $branch);

    # remove the branch we are moving
    $self->_replace_branch(
        key => $key,
        branches => $branches
    ) || return;

    # now perform the actual edit
    $self->_update_report_components( report_name => $report_name, components => $components ) || return;

    return { key => $key };

}

sub update_report_branch {
    my ($self, %args) = @_;
    
    my $name                = $args{'name'};
    my $key                 = $args{'key'};
    my $report_name         = $args{'report_name'};
    my $query               = $args{'query'};
    my $chart_query_params  = $args{'chart_query'};
    my $header_query        = $args{'header_query'};
    my $header_title        = $args{'header_title'};
    my $header_subtitle     = $args{'header_subtitle'};
    my $header_image        = $args{'header_image'};
    my $header_image_name   = $args{'header_image_name'};
    my $component_index     = $args{'component_index'} || 0;
    
    # check that user is defined and not a proxy user 
    $self->_valid_user( $args{'remote_user'} ) || return;
    
    # sanity checks
    if(!defined($key)){
        $self->error( "You must pass in the branches' key." );
        return;
    }
    if(!defined($report_name)){ 
        $self->error( "You must pass in the report_name" ); 
        return;
    }
    if(!defined($component_index)){ 
        $self->error( "You must pass in the component_index" ); 
        return;
    }
    # get this reports' component structure
    my $struct = $self->_get_components( 
        report_name => $report_name,
        component_index => $component_index
    ) || return;
    my $component  = $struct->{'component'};
    my $components = $struct->{'components'};

    $component->{'header'}{'icon'} = undef;

    # get this reports' entire branch structure at the specified component_index
    my $branches = $component->{'branch'};
    if(!$branches){
        $self->error( "The report does not have a branch structure" ); 
        return;
    }

    # get the branch structure with the specified key
    my $branch = $self->_get_branch(
        key => $key,
        branches => $branches
    ) || return;


    # edit the behavior title if it was defined
    if(defined($name)){
        $branch->{'name'} = $name;
    }

    # edit the query if it was passed in 
    if(defined($query)){
        my $query = $self->_parse_json( json => $query, array => 1 ) || return;
        my $chart_query_params  = $self->_parse_json( json => $chart_query_params, array => 1 ) || return;

        $branch->{'query'} = $query;

        # for the time being make assumptions about chart_query stucture
        # branch query
        my $chart_query = $self->_get_chart_query( query => $query->[0] , chart_query => $chart_query_params->[0]) || return;
        $branch->{'chart'}{'query'} = $chart_query;

        # also make assumptions about header query
        my $header_query = $self->_get_header_query( query => $query->[0] ) || return;
        $branch->{'header'}{'query'} = $header_query;
        
    }

    # edit the header query
#    if(defined($header_query)){
#        $branch->{'chart'}{'header'}{'query'} = $header_query; 
#    }
    # edit the header title
    if(defined($header_title)){
        $branch->{'header'}{'title'} = $header_title; 
    }
    # edit the header subsubtitle 
    if(defined($header_subtitle)){
        $branch->{'header'}{'subtitle'} = $header_subtitle; 
    }
    # edit the header image 
    if(defined($header_image)){
        $branch->{'header'}{'image'}      = encode_base64( $header_image, '' );
        $branch->{'header'}{'image_name'} = $header_image_name;
    }

    # now perform the actual edit
    $self->_update_report_components( report_name => $report_name, components => $components ) || return;

    return { key => $key }; 
}

# given a branch query create a chart query structure making some assumptions
sub _get_chart_query {
    my ($self, %args) = @_;
    my $query               = $args{'query'};
    my $chart_query_params  = $args{'chart_query'};

    # get the meta_fields for the chart query
    # for now assume they just want the individual components
    my $required_fields = $self->_get_required_fields( measurement_type => $query->{'from'} ) || return;

    # create value fields array
    # just assume they want to see an aggregate of the average of the same values 
    # that were selected in the branch query for now
    my $value_fields = [];
    foreach my $value_field (@{$query->{'value_field'}}){
        my $aggregate;
        $aggregate = 'min'        if(defined($value_field->{'min'}));
        $aggregate = 'average'    if(defined($value_field->{'average'}));
        $aggregate = 'max'        if(defined($value_field->{'max'}));
        if(defined($value_field->{'percentile'})){
            $aggregate = 'percentile('.$value_field->{'percentile'}.')';
        }
        push(@$value_fields, {
            aggregate => $aggregate, 
            name      => $value_field->{'name'},
            label     => $value_field->{'label'}
        });
    }

    my $chart_query = [{
        from        => $query->{'from'},
        meta_field  => $chart_query_params->{'by'} || $required_fields,
        value_field => $value_fields,
        by          => $chart_query_params->{'by'} || $required_fields 
    }];

    return $chart_query;
}

sub _get_header_query {
    my ($self, %args) = @_;
    my $query = $args{'query'};

    # create value fields array
    # just assume they want to see the total average for the same values 
    # that were selected in the branch query for now
    my $value_fields = [];
    foreach my $value_field (@{$query->{'value_field'}}){
        my $aggregate;
        $aggregate = 'min'        if(defined($value_field->{'min'}));
        $aggregate = 'average'    if(defined($value_field->{'average'}));
        $aggregate = 'max'        if(defined($value_field->{'max'}));
        $aggregate = 'percentile' if(defined($value_field->{'percentile'}));
        
        push(@$value_fields, {
            $aggregate => ($aggregate eq 'percentile') ? $value_field->{'percentile'} : 1,
            name       => $value_field->{'name'},
            label      => $value_field->{'label'}
        });
    }

    my @where = ();

    foreach my $where_field (@{$query->{'where'}}){
        push(@where, $where_field);
    }

    foreach my $by_field (@{$query->{'by'}}){
        push(@where, { field => $by_field,
                        value => { '!=' => undef } } );
    }

    my $chart_query = [{
        from        => $query->{'from'},
        value_field => $value_fields,
        where       => \@where
    }];

    return $chart_query;
}

# used to get the
sub _get_required_fields {
    my ($self, %args) = @_;
    my $measurement_type = $args{'measurement_type'};
    
    my $required_fields = [];
    my $meta_fields = $self->metadata()->get_meta_fields( measurement_type => $measurement_type );
    foreach my $meta_field (@$meta_fields){
        if(defined($meta_field->{'required'}) && $meta_field->{'required'} == 1){
            push(@$required_fields, $meta_field->{'name'});
        }
    }

    return $required_fields;
}
# helper method to get a reports component structure
sub _get_components {
    my ($self, %args) = @_;
   
    my $report_name     = $args{'report_name'};
    my $component_index = $args{'component_index'};

    # get the report's details
    my $report_details = $self->get_report_details( 
        name => $report_name
    );
    if(!@$report_details){
        $self->error( "There is not currently a report name, $report_name." );
        return;
    }
    my $components = $report_details->[0]{'component'};

    if(!$components){
        $self->error( "Error the report does not have a component structure." );
        return;
    }
    
    # get specific component
    my $component = $components->[$component_index];
    if(!$component){
        $self->error( "No component exists at index: $component_index" ); 
        return;
    }

    return {
        report_details => $report_details, 
        components     => $components, 
        component      => $component
    };
}

# a helper method that either retrieves a branch structure with the specified name and the specified depth
# or creates a new one if no name is passed in
sub _get_branch {
    my ($self, %args) = @_;

    my $key      = $args{'key'};
    my $branches = $args{'branches'};

    my @keys = split('-', $key);

    return $self->_get_branch_helper( keys => \@keys, branches => $branches );
}

sub _get_branch_helper {
    my ( $self, %args ) = @_;
    my $keys     = $args{'keys'};
    my $branches = $args{'branches'};

    my $key = shift(@$keys);
   
    my $branch = $branches->[$key];
    if(!$branch){
        $self->error( "Error finding branch with specified key." );
        return;
    }
    if(@$keys == 0){
        return $branch; 
    }
    $self->_get_branch_helper( keys => $keys, branches => $branch->{'branch'} );
}

sub _replace_branch {
    my ($self, %args) = @_;

    my $key      = $args{'key'};
    my $branch   = $args{'branch'};
    my $branches = $args{'branches'};

    my @keys = split('-', $key);

    return $self->_replace_branch_helper( keys => \@keys, branches => $branches, branch => $branch );
}

sub _replace_branch_helper {
    my ( $self, %args ) = @_;
    my $keys     = $args{'keys'};
    my $branch   = $args{'branch'};
    my $branches = $args{'branches'};

    my $key = shift(@$keys);

    if(!$branches->[$key]){
        $self->error( "Error finding branch with specified key." );
        return;
    }
    if(@$keys == 0){
        # delete branch at key position
        splice(@$branches, $key, 1);
        # if branch is defined replace branch at key position with $branch reference passed in
        if(defined($branch)){
            splice(@$branches, $key, $key, $branch);
        }
        
        return 1;
    }
    $self->_replace_branch_helper( keys => $keys, branches => $branches->[$key]{'branch'}, branch => $branch );
}

sub get_report_details {

    my ( $self, %args ) = @_;

    my $name = $args{'name'};

    my $find = $self->format_find(
        field  => 'name',
        values => [$name]
    );

    my $reports = $self->_get_reports( $find )->{'results'};
    if(!$reports || (@$reports < 1) ){
        $self->error( "Access Denied" );
        return;
    }

    my $results = $self->_get_report_details($name);


    if ( !$results ) {
        $self->error( "Error getting report details: $name" );
        return;
    }

    # add keys to each of the branches for editing
    if(@$results > 0 ){
        $self->_add_branch_keys( report_details => $results );
    }

    return $results;

}

sub upload_header_image {
    my ( $self, %args ) = @_;
    my $method_name = $args{'method_name'};
    my $image_fh    = $args{'image'};
    my $name        = $args{'name'};
    
    # check that user is defined and not a proxy user 
    $self->_valid_user( $args{'remote_user'} ) || return;

    #--- Store the image files
    my $image = Image::Magick->new();
    my $err = $image->Read(file=>$image_fh);
    if ( $err ) {
        $self->error( "Error Uploading image: $err" );
        return;
    }

    # resize the image as large-sized
    $image->Resize( geometry => '100x100>' );
    my @img = $image->ImageToBlob();

    $args{'header_image'}      = $img[0];
    $args{'header_image_name'} = $name;
    # remove name since it can conflict with other parameters
    delete $args{'name'};

    if(defined($args{'key'})){
        $self->update_report_branch( %args ) || return;
    }else {
        $self->update_report_component( %args ) || return;
    }

    return { 'name' => $name };
}

# dynamically add keys to each branch that describes how to get there
sub _add_branch_keys {
    my ( $self, %args ) = @_;

    my $report_details = $args{'report_details'};


    foreach my $component (@{$report_details->[0]{'component'}}){
        my $branches = $component->{'branch'};
        $self->_add_branch_keys_helper( branches => $branches );
    }

}

sub _add_branch_keys_helper {
    my ( $self, %args ) = @_;

    my $branches = $args{'branches'};
    my $key      = $args{'key'};
    for(my $index = 0;  $index < @$branches; $index++) {
        my $branch = $branches->[$index];
        my $branch_key = defined($key) ? "$key-$index" : $index;
        $branch->{'key'} = $branch_key;
        if(defined($branch->{'branch'})){
            $self->_add_branch_keys_helper( branches => $branch->{'branch'}, key => $branch_key );
        }
    }
}

# remove all keys from brances
sub _remove_branch_keys {
    my ( $self, %args ) = @_;

    my $components = $args{'components'};
    foreach my $component (@$components){
        my $branches = $component->{'branch'};
        $self->_remove_branch_keys_helper( branches => $branches );
    }
    return 1;
}

sub _remove_branch_keys_helper {
    my ( $self, %args ) = @_;

    my $branches = $args{'branches'};
    foreach my $branch (@$branches){ 
        delete $branch->{'key'};
    }
}


sub get_report_template {

    my ( $self, %args ) = @_;

    my $template_name = $args{'template_name'};
    my $container_id = $args{'container_id'};

    my $results = $self->_get_report_template($template_name,$container_id);

    if ( !$results ) {
        $self->error( $self->error() || "Error getting report details: $template_name" );
        return;
    }

    return $results;

}

sub _add_report {
    my ( $self, %args ) = @_;
    my $report = $args{'report'};

    my $report_collection = $self->mongo_rw()->get_collection("tsds_reports", "reports");

    if (!$report_collection ) {
        $self->error( 'Report DB not available.' );
        return;
    }

    # insert the report
    my $id = $report_collection->insert_one($report);
    if(!$id) {
        $self->error( 'Error creating report' );
        return;
    }

    return [{ name =>  $report->{'name'} }];
}

sub _delete_reports {
    my ( $self, $find ) = @_;
    
    # ensure there are results
    my $reports = $self->_get_reports( $find )->{'results'};
    if(!$reports || (@$reports < 1) ){
        $self->error( "Access Denied" );
        return;
    }

    # get the collection
    my $report_collection = $self->mongo_rw()->get_collection("tsds_reports", "reports");
    if (!$report_collection ) {
        $self->error( 'Report DB not available.' );
        return;
    }

    # remove the reports
    my $success = $report_collection->remove( $find );
    if(!$success) {
        $self->error( 'Error deleting reports' );
        return;
    }
    
    @$reports = map {{ name => $_->{'name'} }} @$reports;
    return $reports; 
}

sub _update_reports {
    my ( $self, $find, $set ) = @_;

    # ensure there are results
    my $reports = $self->_get_reports( $find )->{'results'};
    if(!$reports || (@$reports < 1) ){
        $self->error( "Access Denied" );
        return;
    }

    # get the collection
    my $report_collection = $self->mongo_rw()->get_collection("tsds_reports", "reports");
    if (!$report_collection ) {
        $self->error( 'Report DB not available.' );
        return;
    }

    # remove the reports
    my $success = $report_collection->update_many( $find, {'$set' => $set} );
    if(!$success) {
        $self->error( 'Error updating reports' );
        return;
    }

    @$reports = map {{ name => $_->{'name'} }} @$reports;
    return $reports;
}

sub _update_report_components {
    my ( $self, %args ) = @_;
    my $report_name  = $args{'report_name'};
    my $components   = $args{'components'};

    # access control here? #debugggggg

    my $report_collection = $self->mongo_rw()->get_collection("tsds_reports", "reports");


    if (!$report_collection ) {
        $self->error( 'Report DB not available.' );
        return;
    }

    my $find = $self->format_find(
        field  => 'name',
        values => [$report_name]
    );

    my $reports = $self->_get_reports( $find )->{'results'};
    if(!$reports || (@$reports < 1) ){
        $self->error( "Access Denied" );
        return;
    }

    # clean off any keys that may have been added
    $self->_remove_branch_keys( components => $components ) || return;

    # insert the report
    my $id = $report_collection->update_one({"name" => $report_name}, 
					    {'$set' => { component => $components }} );
    if(!$id) {
        $self->error( 'Error editing report: '.$report_name );
        return;
    }

    return 1;
}

sub _get_reports {

    my $self = shift;
    my $find = shift;
    my $sort = shift || {};

    my $order_by = $sort->{'order_by'} || 'name';
    my $order = $sort->{'order'} || 'asc';
    my $flag = ( lc($order) eq 'asc' ? 1 : -1);

    my $offset = $sort->{'offset'} || 0;
    my $limit = $sort->{'limit'} || 0;


    my $results;
    my $report_collection = $self->mongo_ro()->get_collection("tsds_reports", "reports");
    
    if (! $report_collection ) {
        $self->error( 'Report DB not available.' );
        return;
    }

    my $constraint_key = $self->_get_constraint_key();

    if ($constraint_key ne GLOBAL_VIEW_KEY) {
        $find = $self->format_find(
            field  => 'constraint_key',
            values => [$constraint_key],
            find   => $find
        );
    }

=head
    my $count = $report_collection->find($find)->sort( { $order_by => $flag } )->skip($offset)->limit($limit)->count();

    if ($count <= 0) {
        return;
    }
=cut
    my @reports = $report_collection->find($find)->sort( { $order_by => $flag } )->skip($offset)->limit($limit)->all;
    my $count = $report_collection->count($find);

    @reports = map {{ name           => $_->{'name'}, 
                      type           => $_->{'type'},
                      constraint_key => $_->{'constraint_key'},
                      description    => $_->{'description'} }} @reports;

    $results = \@reports;

    return { results => $results, total => $count };
}

sub _get_report_details  {

    my ( $self, $name ) = @_;

    my $results;
 
    my $report_collection = $self->mongo_ro()->get_collection("tsds_reports", "reports");

    if (! $report_collection ) {
        $self->error( 'Report DB not available.' );
        return;
    }

    my @reports = $report_collection->find({"name" => $name})->all;;
 
    foreach my $report (@reports) {
        delete $report->{'_id'};
    }  

    $results = \@reports;    

    return $results;
}

sub _get_report_template {

    my ( $self, $template_name, $container_id ) = @_;

    my $results;
    my $config = $self->{'config'};
    my $output;

    my $file_dir = $config->get('/config/report-root');
    my $file     = $file_dir.$template_name;

    if($file !~ /\.tt$/) {
        $file .= '.tt';
    }

    my $vars = { container_id => $container_id };
    my $template = Template->new({ ABSOLUTE => 1 });

    $template->process($file, $vars, \$output)
        or do { $self->error("Template process failed: ". $template->error());
                return; };

    $results = [{template => $output}];

    return $results;
}

sub metadata {

    my ( $self, $metadata ) = @_;

    $self->{'metadata'} = $metadata if ( defined( $metadata ) );

    return $self->{'metadata'};
}

sub _valid_user {
    my ( $self, $remote_user ) = @_;
    if(!defined($remote_user) || grep(/^$remote_user$/, @{$self->{'proxy_users'}})){
        $self->error( "Invalid User.");
        return;
    }

    return 1;
}

sub _parse_json {
    my ( $self, %args ) = @_;

    my $json_blob = $args{'json'};
    my $array     = $args{'array'} || 0;

    my $json;
    eval {
        $json = decode_json($json_blob);
    };
    if($@){
        $self->error( "Error decoding json: ".$@ );
        return;
    }

    my $reftype = ref $json;
    if($array && $reftype ne 'ARRAY'){
        $self->error( "Error expected json ARRAY and got $reftype");
        return;
    }
    return $json;
}

sub _get_constraint_key {
    my ( $self ) = @_;

    if  ( defined($self->{'global_uri'}) and $self->{'global_uri'} ne '' ) {
        return GLOBAL_VIEW_KEY;
    }
 
    my $host_name = $ENV{'HTTP_HOST'};
    my $uri       = $ENV{'REQUEST_URI'};

    my $u = URI->new($uri); 
    my $path  = "";

    my @es = split('/', $u->path);

    foreach my $e (@es) {
        if (length($e) > 0 and ($e !~ /\.cgi/)) {
            $path .= "/".$e;
        }
    }

    return $host_name.$path;
}

1;

