#--------------------------------------------------------------------
#----- GRNOC TSDS Aggregation DataService Library
#-----
#----- Copyright(C) 2015 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/DataService/Aggregation.pm $
#----- $Id: Aggregation.pm 35325 2015-02-13 19:15:28Z mj82 $
#-----
#----- This module inherits the base GRNOC::TSDS::DataService class
#----- and provides all of the methods to interact with aggregations
#--------------------------------------------------------------------

package GRNOC::TSDS::DataService::Aggregation;

use strict;
use warnings;

use base 'GRNOC::TSDS::DataService';

use GRNOC::Log;
use GRNOC::TSDS::MongoDB;
use GRNOC::TSDS::Parser;

use Tie::IxHash;
use DateTime;
use DateTime::Format::Strptime;
use Data::Dumper;
use JSON qw( decode_json );

### constants ###
use constant DEFAULT_COLLECTIONS => ['data', 'measurements', 'metadata', 'aggregate', 'expire'];

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

    $self->parser( GRNOC::TSDS::Parser->new( @_ ) );

    return $self;
}

# GET METHODS
sub get_aggregations {
    my ( $self, %args ) = @_;
    
    my $meta_fields;
    my @aggregation_fields;

    my $measurement_type = $args{'measurement_type'};

    my $aggregate_collection = $self->mongo_ro()->get_collection($measurement_type, "aggregate");
    if (! $aggregate_collection ) {
        $self->error( 'Invalid Measurement Type.' );
        return;
    }

    my $aggregates = $aggregate_collection->find();
    if (! $aggregates ) {
        $self->error( 'Invalid Measurement Type: no aggregations found.' );
        return;
    }
    my @aggregate_results = @{$self->_get_agg_exp_fields($aggregates)};

    my @new_results = sort by_blanklast ( @aggregate_results );

    return \@new_results;
}

sub get_expirations {
    my ( $self, %args ) = @_;

    my $measurement_type = $args{'measurement_type'};
    
    my $expiration_collection = $self->mongo_ro()->get_collection($measurement_type, "expire");
    if (! $expiration_collection ) {
        $self->error( 'Invalid Measurement Type.' );
        return;
    }

    my $expirations = $expiration_collection->find();
    if (! $expirations ) {
        $self->error( 'Invalid Measurement Type: no expirations found.' );
        return;
    }
    my @expiration_results = @{$self->_get_agg_exp_fields($expirations)};
    
    my @new_results = sort by_blanklast ( @expiration_results );
    
    return \@new_results;

}

# UPDATE METHOD
sub update_aggregations {

    my ( $self, %args ) = @_;
    
    my $measurement_type  = $args{'measurement_type'};
    my $meta              = $args{'meta'};
    my $name              = $args{'name'};
    my $new_name          = $args{'new_name'};
    my $max_age           = $args{'max_age'}; 
    my $eval_position     = $args{'eval_position'}; 
    my $values            = $args{'values'};

    # convert numeric params to ints
    $eval_position = int $eval_position if(defined($eval_position));
    $max_age       = int $max_age       if(defined($max_age));

    my $query = {'name'=> $name};

    if (!defined($name) || $name eq '') {
        $self->error("You must specify a name to update an aggregation/expiration.");
        return;
    }

    if (exists($args{'new_name'}) && (!defined($new_name) || $new_name eq '')) {
        $self->error("You must enter text for the new_name field");
        return;
    }

    if (defined($values)){
        return if (!$self->_validate_values($values, $measurement_type));
    }

    # get the aggregate collection
    my $agg_col = $self->mongo_rw()->get_collection($measurement_type, "aggregate");
    if(!$agg_col){
        $self->error($self->mongo_rw()->error());
        return;
    }
    
    # make sure this aggregate record exists
    if(!$self->_agg_exp_exists( col => $agg_col, name => $name )){
        $self->error("Aggregation named $name doesn't exist");
        return;
    }
    
    # reorder eval positions
    if(defined($eval_position)){
        my $position_res = $self->_update_eval_positions(
            collection => $agg_col, 
            name => $name,
            eval_position => $eval_position
        );
    }
    
    my $set = {};
    my $id;
    $set->{'meta'}          = $meta      if(exists($args{'meta'}));  
    $set->{'values'}        = $values    if(exists($args{'values'})); 
    $set->{'name'}          = $new_name  if(exists($args{'new_name'}));  
    if(!%$set && !exists($args{'eval_position'})){
        $self->error( "You must pass in at least 1 field to update" );
        return;
    }
    
    if(%$set){
        $id = $agg_col->update_one($query, { '$set' => $set } );
        if(!$id) {
            $self->error( "Error updating values in aggregate with name $name");
            return;
        }
    }

    return [{ 'success' => 1 }];
}

sub update_expirations {

    my ( $self, %args ) = @_;
    
    my $measurement_type  = $args{'measurement_type'};
    my $meta              = $args{'meta'};
    my $name              = $args{'name'};
    my $new_name          = $args{'new_name'};
    my $max_age           = $args{'max_age'}; 
    my $eval_position     = $args{'eval_position'}; 
    my $values            = $args{'values'};
    
    # convert numeric params to ints
    $eval_position = int $eval_position if(defined($eval_position));
    $max_age       = int $max_age       if(defined($max_age));

    if (!defined($name) || $name eq '') {
        $self->error("You must specify a name to update an aggregation/expiration.");
        return;
    }
    if (exists($args{'new_name'}) && (!defined($new_name) || $new_name eq '')) {
        $self->error("You must enter text for the new_name field");
        return;
    }
    
    # get the expire collection
    my $exp_col = $self->mongo_rw()->get_collection($measurement_type, "expire");
    if(!$exp_col){
        $self->error($self->mongo_rw()->error());
        return;
    }
    
    # make sure this aggregate record exists
    if(!$self->_agg_exp_exists( col => $exp_col, name => $name )){
        $self->error("Expiration named $name doesn't exist");
        return;
    }
    
    # reorder eval positions
    if(defined($eval_position)){
        my $position_res = $self->_update_eval_positions(
            collection => $exp_col,
            name => $name,
            eval_position => $eval_position
        );
    }

    
    # figure out which fields were modifying for the expire record
    my $set = {};
    $set->{'meta'}    = $meta     if(exists($args{'meta'}));     
    $set->{'max_age'} = $max_age  if(exists($args{'max_age'})); 
    $set->{'name'}    = $new_name if(exists($args{'new_name'}));  

    # if it's the default expire record don't allow them to edit anything but max_age
    if($name eq 'default'){
        foreach my $field (keys %$set){
            if($field ne 'max_age'){
                $self->error( "You can only edit the max_age on the default expire record");
                return; 
            }
        }
    }

    if(%$set){
        my $id = $exp_col->update_one({ name => $name }, { '$set' => $set } );
        if(!$id) {
            $self->error( "Error updating values in expiration with name $name");
            return;
        } 
    }

    return [{ 'success' => 1 }];
}

# INSERT METHOD
sub add_aggregation {

    my ( $self, %args ) = @_;
    
    my $measurement_type = $args{'measurement_type'};
    my $interval         = $args{'interval'};
    my $meta             = $args{'meta'};
    my $name             = $args{'name'};
    my $values           = $args{'values'};

    #sanity checks
    if (!defined($name) || $name eq '') {
        $self->error("You must specify a name for the aggregation.");
        return;
    }
    if (defined($values)){
	    return if (! $self->_validate_values($values, $measurement_type));
    }
    if(!defined($interval)){
        $self->error("You must specify an interval to aggregate the data on");
        return;
    }

    my $set = {};
    $set->{'interval'} = int($interval) if(defined($interval));
    $set->{'name'}     = $name          if(defined($name));
    $set->{'values'}   = $values        if(defined($values));

    # meta might not be passed in, it needs to be set to empty object to avoid problem with deletion
    if(defined($meta)) {
        $set->{'meta'} = $meta;
    }
    else {
        $set->{'meta'} = "{}";
    }

    # get the aggregate collections
    my $agg_col = $self->mongo_rw()->get_collection($measurement_type, "aggregate");
    if(!$agg_col){
        $self->error($self->mongo_rw()->error());
        return;
    }

    # make sure this aggregation doesn't already exists
    if($self->_agg_exp_exists( col => $agg_col, name => $name )){
        $self->error("Aggregation named, $name, already exist");
        return;
    }

    # figure out the highest eval_position currently used (if any)
    my $highest_eval_position = $self->_agg_highest_eval_position( col => $agg_col );
    my $new_eval_position = $highest_eval_position + 10;
    $set->{'eval_position'} = $new_eval_position;

    # create the data_[interval] collection
    if(!$self->mongo_root()->add_collection_shard( $measurement_type, "data_$interval" , $GRNOC::TSDS::MongoDB::DATA_SHARDING )){
        $self->error( "Error adding collection shard for data_$interval measurement_type: ".$self->mongo_rw()->error() );
        return;
    }
    my $agg_data_col = $self->mongo_rw()->get_collection( $measurement_type, "data_$interval", create => 1 );
    my $indexes = $agg_data_col->indexes();
    $indexes->create_one([start => 1]);
    $indexes->create_one([end   => 1]);
    $indexes->create_one([updated => 1, identifier => 1]);
    $indexes->create_one([identifier => 1, start => 1, end => 1]);

    my $id = $agg_col->insert_one($set);
    if(!$id) {
        $self->error( "Error inserting values in aggregate with interval $interval and meta $meta");
        return;
    } 

    return [{ 'success' => 1 }];
}

sub add_expiration {

    my ( $self, %args ) = @_;
    
    my $measurement_type = $args{'measurement_type'};
    my $interval         = $args{'interval'};
    my $meta             = $args{'meta'};
    my $name             = $args{'name'};
    my $max_age          = $args{'max_age'};
    
    #sanity checks
    if (!defined($name) || $name eq '') {
        $self->error("You must specify a name for the expiration.");
        return;
    }
    if(!defined($max_age)){
        $self->error("You must specify the max_age of the data of the expiration.");
        return;
    }
    
    my $set = {};
    $set->{'interval'} = int($interval) if(defined($interval));
    $set->{'meta'}     = $meta          if(defined($meta));
    $set->{'name'}     = $name          if(defined($name));
    $set->{'max_age'}  = int($max_age)  if(defined($max_age));

    # if they've set an interval make sure an aggregation with the same interval exists
    # (we can't expire aggregated data that doesn't exists)
    if(defined($interval)){
        my $found_interval = 0;
        my $aggregations = $self->get_aggregations( measurement_type => $measurement_type );
        foreach my $aggregation (@$aggregations){
            next if($aggregation->{'interval'} ne $interval);
            $found_interval = 1;
            last;
        }
        if(!$found_interval){
            $self->error("Can not add expiration at interval $interval. There must be an aggregation at interval, $interval to expire");
            return;
        }
    }

    my $exp_col = $self->mongo_rw()->get_collection($measurement_type, "expire");
    if(!$exp_col){
        $self->error($self->mongo_rw()->error());
        return;
    }
    # make sure this expiration doesn't already exists
    if($self->_agg_exp_exists( col => $exp_col, name => $name )){
        $self->error("Expiration named, $name, already exist");
        return;
    }

    # figure out the highest eval_position currently used (if any)
    my $highest_eval_position = $self->_agg_highest_eval_position( col => $exp_col );
    my $new_eval_position = $highest_eval_position + 10;
    $set->{'eval_position'} = $new_eval_position;

    my $id = $exp_col->insert_one( $set );
    if(!$id) {
        $self->error( "Error inserting values in expiration with interval $interval and meta $meta");
        return;
    } 
    
    return [{ 'success' => 1 }];
}

sub _agg_exp_exists {
    my ( $self, %args ) = @_;
    my $col        = $args{'col'};
    my $name       = $args{'name'};

    # make sure a agg doesn't already exist with this name
    my $count = $col->count({ name => $name });
    return 1 if $count;    
    return 0;
}

sub _agg_highest_eval_position {

    my ( $self, %args ) = @_;

    my $col = $args{'col'};
    my @aggregates = $col->find( {} )->all();

    my $highest_eval_position = 0;

    foreach my $aggregate ( @aggregates ) {

	my $eval_position = $aggregate->{'eval_position'};
	
	if ( $eval_position && $eval_position > $highest_eval_position ) {

	    $highest_eval_position = $eval_position;
	}
    }

    return $highest_eval_position;
}

# DELETE METHOD
sub delete_aggregations {
    my ( $self, %args ) = @_;
    
    my $measurement_type = $args{'measurement_type'};
    my $name = $args{'name'};

    # sanity checks
    if (!defined($name) || $name eq '') {
        $self->error("You must specify a name to delete an aggregation/expiration.");
        return;
    }

    # get the aggregate collection
    my $agg_col = $self->mongo_rw()->get_collection($measurement_type, "aggregate");
    if(!$agg_col){
        $self->error($self->mongo_rw()->error());
        return;
    }
    # make sure the aggregate rule with this name exists
    if(!$self->_agg_exp_exists( col => $agg_col, name => $name )){
        $self->error("Aggregation named, $name, doesn't exist");
        return;
    }

    # remove the data_$interval collection
    my $interval     = $agg_col->find({ name => $name })->next()->{'interval'};
    my $agg_data_col = $self->mongo_rw()->get_collection($measurement_type, "data_$interval");

    # now delete the relevant data from the aggregate data collection and possbilly the whole
    # collection if no data if left after the delete
    $self->_delete_aggregation_data(
        interval         => $interval,
        measurement_type => $measurement_type,
        agg_col          => $agg_col, 
        agg_data_col     => $agg_data_col, 
        name             => $name
    ) || return;

    # remove the aggregate rule from the collection
    my $id = $agg_col->delete_one({name => $name});
    if(!$id) {
        $self->error( "Error removing aggregate rule for $name.");
        return;
    } 
    
    # get the related expire rule and remove it from the expire collection
    my $exp_col = $self->mongo_rw()->get_collection($measurement_type, "expire");
    if(!$exp_col){
        $self->error($self->mongo_rw()->error());
        return;
    }
    $id = $exp_col->delete_one({ name => $name });
    if(!$id) {
        $self->error( "Error removing values from expiration with name $name.");
        return;
    } 

    return [{ 'success' => 1 }];
}

sub _delete_aggregation_data {
    my ( $self, %args ) = @_;
    my $interval         = $args{'interval'};
    my $measurement_type = $args{'measurement_type'};
    my $agg_col          = $args{'agg_col'};
    my $agg_data_col     = $args{'agg_data_col'};
    my $name             = $args{'name'}; # the name of the aggregation being deleted

    # build an array of all of the meta data from the aggregations we're not deleting 
    # within this interval
    my $nor = [];
    my $cur = $agg_col->find({});
    while (my $agg = $cur->next()) {
        next if($name     ne $agg->{'name'});
        next if($interval ne $agg->{'interval'});
        my $meta;
        eval {
            $meta = decode_json( $agg->{'meta'} );
        };
        if($@){
            $self->error("Problem decoding meta scope for aggregate ".$agg->{'name'}.": $@");
            return;
        }
        push(@$nor, $meta);
    }

    # grab the measurement collection for this measurement_type
    my $meas_col = $self->mongo_rw()->get_collection($measurement_type, "aggregate");
    if(!$meas_col){
        $self->error($self->mongo_rw()->error());
        return;
    }

    # now find all the identifiers that do not match that meta data
    # of the remaining aggregations
    my $ids = [];
    if(@$nor){
        $cur = $meas_col->find({ '$nor' => $nor }, { identifier => 1 });
        while (my $meas = $cur->next()) {
            push(@$ids, $meas->{'identifier'});
        }
    }

    # if there's other aggregations besides the one we are deleting
    # delete everything in data_$interval that doesn't match their metadata scope
    if(@$ids){
        my $res = $agg_data_col->delete_many({ identifier => { '$in' => $ids } });
        if(!$res) {
            $self->error( "Error removing values from aggregate with name $name.");
            return;
        } 
    }

    # if there's no data left in the agg data cursor drop it
    if ($agg_data_col->count({}) == 0) {
        $agg_data_col->drop();
    }
    
    return 1;
}

sub delete_expirations {
    my ( $self, %args ) = @_;
    
    my $measurement_type = $args{'measurement_type'};
    my $name = $args{'name'};
    
    # sanity checks
    if (!defined($name) || $name eq '') {
        $self->error("You must specify a name to delete an aggregation/expiration.");
        return;
    }
    if ($name eq 'default'){
        $self->error("You can not delete the default expire rule.");
        return;
    }

    # get the expire rule and remove it from the expire collection
    my $exp_col = $self->mongo_rw()->get_collection($measurement_type, "expire");
    if(!$exp_col){
        $self->error($self->mongo_rw()->error());
        return;
    }
    
    # make sure the aggregate rule with this name exists
    if(!$self->_agg_exp_exists( col => $exp_col, name => $name )){
        $self->error("Aggregation named, $name, doesn't exist");
        return;
    }
    my $id = $exp_col->delete_one({name => $name});
    if(!$id) {
        $self->error( "Error removing values from expiration with name $name.");
        return;
    } 
    
    return [{ 'success' => 1 }];
}

sub _get_agg_exp_fields {
    my ($self, $cursor) = @_;

    my @results = ();
    while (my $doc = $cursor->next()) {
        my %row;
        foreach my $key (keys %$doc) {
            next if $key eq '_id';
            my $value = $doc->{$key};
            $row{$key} = $value;
        }
        push @results, \%row;
    }
    return \@results;
}

sub _update_eval_positions {

    my ($self, %args) = @_;

    my $col               = $args{'collection'};
    my $name              = $args{'name'};
    my $new_eval_position = $args{'eval_position'} || 10;

    my $query = {'name' => $name};
    my $old_eval_position = $self->_get_eval_position( col => $col, name => $name);

    # see if there is another rule with the same eval_position
    my $same_eval_position = $self->_eval_position_in_use(
        'eval_position' => $new_eval_position, 
        'name', => $name,
        'col' => $col
    );
    
    # if this eval position isn't in use by another rule
    if (!$same_eval_position && ($old_eval_position == $new_eval_position)) {
        return { 'success' => 1 };
    }
   
    # see if there are values (other than this one) that
    # lack eval_positions 
    my $has_empty_values =  $self->_has_null_eval_positions(
        'name' => $name,
        'col' => $col

    );

    # if there is no conflict, and there are no other null values,
    # just update the current rule
    if (!$same_eval_position && !$has_empty_values) {

        my $result = $self->_set_eval_position(
            'eval_position' => $new_eval_position, 
            'name' => $name,
            'col' => $col
        );

    # if there is a conflict, we need to reorder
    } else {

        my $result = $self->_recalculate_eval_positions(
            'old_eval_position' => $old_eval_position, 
            'new_eval_position' => $new_eval_position, 
            'name' => $name,
            'col' => $col

        );

    }

}

sub _has_null_eval_positions {
    my ($self, %args) = @_;
    my $name = $args{'name'};
    my $col = $args{'col'};

    my $query = { 'eval_position' => { '$exists' => 0 }, 'name' => { '$ne' => $name } };

    if ($col->count($query)) {
        return 1;
    }
    return 0;
}

sub _recalculate_eval_positions {

    my ( $self, %args ) = @_;

    my $new_eval_position = $args{'new_eval_position'};
    my $old_eval_position = $args{'old_eval_position'};
    my $name = $args{'name'};
    my $col = $args{'col'};

    my $query = { 'name' => $name };

     # these are the other docregations that didn't get updated / aren't getting their position replaced
    my $other_cur = $col->find( { 'eval_position' => {'$ne' => $new_eval_position}, 
       'name' => {'$ne' => $name} } );
    my $other_docs = [];

    # detect error
    return if ( !defined( $other_docs ) );

    while (my $doc = $other_cur->next()) {
        push @$other_docs, $doc;
    }

    # get the other docregations in the table that are getting their position replaced

    my $replaced_docs = [];
    my $replaced_cur = $col->find( {'eval_position' => $new_eval_position, 'name' => {'$ne' => $name} } );

    while (my $doc = $replaced_cur->next()) {
        my %row = ();
        push @$replaced_docs, $doc;
    }

    # detect error
    return if ( !defined( $replaced_cur ) );

    my $updated_doc = $col->find_one( $query );

    $updated_doc->{'eval_position'} = $new_eval_position;

    return if ( !defined( $updated_doc ) );

    # does the updated rule need to go *below* the rule its taking place of? (drdocing down
    # or there is no old eval position)
    if (defined($old_eval_position) && $new_eval_position > $old_eval_position ) {
        push( @$replaced_docs, $updated_doc );
    } else {
        # the update rule needs to go *above* the rule its taking place of. (drdocing up)

        unshift( @$replaced_docs, $updated_doc );
    }

    # generate the new full list in the correct order
    my @new_list = sort by_blanklast ( @$other_docs, @$replaced_docs );

    # update every rule's eval_position from 10 .. based upon the new order
    my $i = 10;

    foreach my $rule ( @new_list ) {
        #warn 'updating ' . $rule->{'name'} . ' to eval position: ' . $i;
        my $update_query = { 'name' => $rule->{'name'} };
        my $set = { 'eval_position' => $i };
        my $exp_res = $col->update_one($update_query, {'$set' => $set });
        $i += 10;
    }
}

sub _set_eval_position {
    my ( $self, %args ) = @_;
    my $eval_position = $args{'eval_position'};
    my $name = $args{'name'};
    my $col = $args{'col'};

    my $query = { 'name' => $name };
    my $set = { 'eval_position' => $eval_position };

    
    my $exp_res = $col->update_one($query, { '$set' => $set });


    if (!$exp_res) {
        return 0;
    }

    return 1;

}

sub _eval_position_in_use {
    my ( $self, %args ) = @_;
    my $eval_position = $args{'eval_position'};
    my $name = $args{'name'};
    my $col = $args{'col'};

    my $query = { 'eval_position' => $eval_position, 'name' => {'$ne' => $name} };

    my $exp_res = $col->find($query);
    my $in_use  = $col->count();

    return $in_use;

}

sub _get_eval_position {my ( $self, %args ) = @_;
    my $col        = $args{'col'};
    my $name       = $args{'name'};

    # make sure the collection/name exists
    my $result = $col->find_one({ name => $name });
    if(!$result){
        return;
    }
    my $eval_position = $result->{'eval_position'};
    return $eval_position;
    
}

sub _validate_values {
    my $self = shift;
    my $obj  = shift;
    my $type = shift;

    if (ref $obj ne 'HASH'){
	$self->error("values must be an object");
	return;
    }

    my $metadata = $self->mongo_rw()->get_collection($type, 'metadata');
    if (! $metadata){
	$self->error($self->mongo_rw()->error());
	return;
    }

    $metadata = $metadata->find_one();

    # Make sure each value exists and that the values we're passing
    # in for aggregation configuration make sense
    foreach my $value_name (keys %$obj){
	if (! exists $metadata->{'values'}{$value_name}){
	    $self->error("Unknown value \"$value_name\"");
	    return;
	}
	foreach my $key (keys %{$obj->{$value_name}}){
	    my $key_value = $obj->{$value_name}{$key};

	    # Make sure we only passed in keys that we know about
	    if ($key ne 'hist_res' && $key ne 'hist_min_width'){
		$self->error("Unknown value field \"$key\" for value \"$value_name\"");
		return;
	    }

	    # A null value is okay
	    if (! defined $key_value || $key_value eq ''){
		$obj->{$value_name}{$key} = undef;
	    }

	    # Make sure they are numbers
	    else {
		if ($key_value !~ /^\d+(\.\d+)?$/){
		    $self->error("Value field \"$key\" for value \"$value_name\" must be a number");
		    return;
		}
		
		# Make sure the fields are sane
		if ($key eq 'hist_res'){
		    if ($key_value >= 100 || $key_value <= 0){
			$self->error("hist_res for value \"$value_name\" must be between 0 and 100");
			return;
		    }
		}
		elsif ($key eq 'hist_min_width'){
		    if ($key_value <= 0){
			$self->error("hist_min_width for value \"$value_name\" must be greater than 0");
			return;
		    }
		}
	    }
	}
    }

    return 1;
}

# sort by eval_position, putting the rows that lack an eval_position
# at the bottom
sub by_blanklast {
    # if only one object doesn't have an eval position set put the object
    # without an eval position at the end
    if (!exists($a->{'eval_position'}) ^ !exists($b->{'eval_position'})){
        return exists($b->{'eval_position'}) - exists($a->{'eval_position'});
    }
    # if both objects don't have an eval position set sort by name
    elsif(!exists($a->{'eval_position'}) && !exists($b->{'eval_position'})){
        return $a->{'name'} cmp $b->{'name'};
    }
    # otherwise just sort by the eval position
    return $a->{'eval_position'} cmp $b->{'eval_position'};
}



1;
