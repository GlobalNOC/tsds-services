#!/usr/bin/perl -I /opt/grnoc/venv/grnoc-tsds-services/lib/perl5
#--------------------------------------------------------------------
#----- GRNOC TSDS Search DataService Library
#-----
#----- Copyright(C) 2015 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/DataService/Search.pm $
#----- $Id: Search.pm 36477 2015-04-07 19:02:57Z mj82 $
#-----
#----- This module inherits the base GRNOC::TSDS::DataService class
#----- and provides all of the methods to interact with metadata
#--------------------------------------------------------------------

package GRNOC::TSDS::DataService::Search;

use strict;
use warnings;

use base 'GRNOC::TSDS::DataService';

use constant MAX_SEARCH_RESULTS => 10_000;
use constant SPH_RANK_SPH04     => 'sph04';

use GRNOC::Log;
use GRNOC::TSDS::MongoDB;
use GRNOC::TSDS::Parser;
use GRNOC::TSDS::Constants;
use GRNOC::TSDS::DataService::MetaData;

use DBI;
use JSON qw( decode_json );
use Storable qw(dclone);
use DateTime;
use DateTime::Format::Strptime;
use Data::Dumper;
use JSON;
use Time::HiRes;
use List::MoreUtils qw( uniq );

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

    # create and store config object
    my $config = GRNOC::Config->new( 
        config_file => $self->{'config_file'},
        force_array => 0
    );
    if($config->{'error'}){
        warn "Error parsing config in TSDS::DataService::Search: ".$config->{'error'}{'msg'};
        return;
    }

    my $sphinx_host = $config->get('/config/sphinx/mysql/@host') || '127.0.0.1';
    my $sphinx_port = $config->get('/config/sphinx/mysql/@port') || 9306;
    
    my $dbh = DBI->connect("dbi:mysql:database=;host=$sphinx_host;port=$sphinx_port", "", "",{mysql_no_autocommit_cmd => 1});
    if(!$dbh){
        warn "Failed to connect to sphinx via DBI";
        return;
    }
    $self->dbh( $dbh );

    $self->metadata( GRNOC::TSDS::DataService::MetaData->new( @_ ) );

    return $self;
}

# SEARCH METHOD
sub search {
    my ($self, %args) = @_;

    my $search             = $args{'search'};
    my $limit              = $args{'limit'}  || 100;
    my $offset             = $args{'offset'} || 0;
    my $aggregator         = $args{'aggregator'} || 'average';
    my $step               = $args{'step'} || 3600;
    my $order              = $args{'order'} || 'asc';
    my $order_by           = $args{'order_by'};
    my $meta_field_names   = $args{'meta_field_name'};
    my $meta_field_values  = $args{'meta_field_value'};
    my $meta_field_logics  = $args{'meta_field_logic'};
    my $value_field_names  = $args{'value_field_name'};
    my $value_field_values = $args{'value_field_value'};
    my $value_field_logics = $args{'value_field_logic'};
    my $value_field_functions = $args{'value_field_function'};
    my $group_by           = $args{'group_by'};
    my $types              = $args{'measurement_type'};

    # determine if we're skipping the sphinx search part
    my $skip_sphinx_search = 0;
    if(!defined($search) && !defined($meta_field_names)){
        $skip_sphinx_search = 1;
    }
    # if we're going to be searching based on a value, we have to skip sphinx
    # since values aren't shared across types
    if(defined $value_field_names && @$value_field_names > 0){
        $skip_sphinx_search = 1;
    }

    # return an error if the search string is defined and less than 3 characters long.
    # otherwise sphinx will just not return any results which is misleading
    if(defined($search) && length($search) < 3){
        $self->error("Can not perform search on term '$search'. Search term must be at least 3 characters long.");
        return;
    }

    # get query start time 
    my $run_start_time = Time::HiRes::gettimeofday();

    my $date_format = DateTime::Format::Strptime->new( pattern => '%m/%d/%Y %H:%M:%S');

    # if nothing was passed in for end time default to now
    my $end_epoch = $args{'end_time'} || time; 
    my $end_time  = $date_format->format_datetime(
        DateTime->from_epoch( epoch => $end_epoch )
    );

    # if nothing was passed in for start set to end - 1day
    my $start_epoch = $args{'start_time'}  || DateTime->from_epoch( epoch => $end_epoch)->subtract( days => 1 )->epoch();
    my $start_time  = $date_format->format_datetime(
        DateTime->from_epoch( epoch => $start_epoch )
    );
   
    # filter by passed in measurement_types or query them all
    if(!defined($types) || !@$types){
        my $measurement_types = $self->metadata()->get_measurement_types();
        foreach my $type (@$measurement_types) {
            my $name = (defined($type->{'parent'})) ? $type->{'parent'}.'.'.$type->{'name'} : $type->{'name'};
            push(@$types, $name);
        }
    }

    # get the schemas for each measurement type where querying on
    my $schemas = $self->metadata()->get_measurement_type_schemas( 
        # only send the parent classifiers when getting the schemas
        measurement_type => [uniq(map {
            my ($classifier, $measurement_type) = $self->metadata()->_parse_measurement_type( $_ );
            $measurement_type;
        } @$types)]
    );

    # if the user passed in field specific matches / logic, create the appropriate sphinx search string
    my $per_field_search_string;
    if(defined($meta_field_names) && defined($meta_field_values) && defined($meta_field_logics)){
        if( (@$meta_field_names != @$meta_field_values) || (@$meta_field_values != @$meta_field_logics) ){
            $self->error('Must pass in the same number of meta_field_name, meta_field_value, and meta_field_logic parameters.');
            return;
        }
        $per_field_search_string = $self->_create_per_field_search(
            measurement_types => $types,
            meta_field_names  => $meta_field_names,
            meta_field_values => $meta_field_values,
            meta_field_logics => $meta_field_logics
        );
    }

    # sanity check the search by value options if present
    if(defined($value_field_names) && defined($value_field_values) && defined($value_field_logics) && defined($value_field_functions)){
        if( (@$value_field_names != @$value_field_values) || (@$value_field_values != @$value_field_logics) || (@$value_field_values != @$value_field_functions) ){
            $self->error('Must pass in the same number of value_field_name, value_field_value, value_field_logic, and value_field_function parameters.');
            return;
        }
    }

    # don't allow the user to search in more than 1 measurement_type with a undef search term
    # this will cause a non-determisitic number of results when limit / offset is used
    # also skipping sphinx if we need to search by value which is also limited to 1 type
    if($skip_sphinx_search && (@$types > 1)){
        $self->error("You can not search on more than one measurement_type with an undefined search term or when searching by values");
        return;
    }
    
    # don't allow the user to group_by custom fields if more than one measurement_type is being operated on 
    if(defined($group_by) && (@$types > 1)){
        $self->error("You can not use the group_by parameter when more than one measurement_type is being searched on");
        return;
    }

    # don't allow the user to sort by fields with more than one measurement_type
    if((@$types > 1) && (defined($order_by) && @$order_by)){
        $self->error("You can not order by fields when searching across more than one measurement type");
        return;
    }

    # if the user wants to sort by a field and we are only searching on one measurement type
    # skip the sphinx phase. otherwise you'll have to wait until the mongo phase to do the limit and offset
    # and parsing the where clause generated from the unlimited meta_ids returned by sphinx will time out
    # at the mongo phase 
    if( (@$types == 1) && (defined($order_by) && @$order_by)){
        $skip_sphinx_search = 1;
    }

    # loop through the types we're querying building our search weights hash
    # and our coniditions hash to be applied to the data query
    my $weights = {};
    my $conditions_by_type = {};
    foreach my $type (@$types) {
        my $measurement_type = $self->metadata()->_parse_measurement_type( $type );
        my $db = $self->mongo_ro()->get_database($measurement_type);

        # get the flat list of this measurement types' meta fields
        my $metadata = $db->get_collection( 'metadata' )->find_one({});
        if(!defined($metadata)){
            log_info("$measurement_type has no metadata collection, skipping...");
            next;
        }
        my $meta_fields = $self->mongo_ro()->flatten_meta_fields( 
            prefix => '', 
            data   => $metadata->{'meta_fields'}
        );

        # only create these conditional fields if we are doing a text search
        $conditions_by_type->{$type} = [];
        if(defined($search)){
            my @and_fields;
            # split original search term on spaces and AND results together
            foreach my $term (split(' ', $search)){
                my @or_fields;
                foreach my $meta_field (keys %$meta_fields){
                    push(@or_fields, $meta_field.' like "'.$term.'"');
                }
                push(@and_fields, '( '.join(' or ', @or_fields).' )');
            }
            my $search_string = (@and_fields > 1) ? 
                '( '.join(' and ', @and_fields).' )' : $and_fields[0];
            push(@{$conditions_by_type->{$type}}, $search_string);
        }

        # loop through the list of meta fields storying condition filters for the data request later
        # and the weights for the sphinx query
        foreach my $meta_field (keys %$meta_fields){
            # pull out our field and measurement type weight as defined in our tsds metadata
            # and create the appropriate sphinxql weight string for them
            my $type_weight  = $metadata->{'search_weight'} || 1;
            my $field_weight = $meta_fields->{$meta_field}{'search_weight'} || 0;

            (my $type_underscore = $type) =~ s/\./_/g;
            $weights->{$type_underscore."__".$meta_field} = $type_weight + $field_weight;
        }

        # loop through the list of specific meta field filters the user passed in (if any),
        # storing condition filters for the data request later as well ass the weights for 
        # the sphinx query. only create these conditionals if we are doing per field searches
        if(defined($meta_field_names)){
            my @and_fields;
            for(my $i = 0; $i < @$meta_field_names; $i++){
                my $meta_field_name = $meta_field_names->[$i];
                # if the user passed in  meta_field doesn't exist for this type just skip it
                my $meta_field = $meta_fields->{$meta_field_name} || next;

                my $meta_field_value = $meta_field_values->[$i];
                my $meta_field_logic = $meta_field_logics->[$i];

                my $operator;
                if($meta_field_logic eq 'is'){
                    $operator = '=';
                } 
                elsif($meta_field_logic eq 'is_not'){
                    $operator = '!=';
                } 
                elsif($meta_field_logic eq 'contains'){
                    $operator = 'like';
                } 
                elsif($meta_field_logic eq 'does_not_contain'){
                    $operator = 'not like';
                }
                else {
                    $self->error("Do not know how to handle logic, $meta_field_logic");
                    return;
                }
            
                # create the tsdsql equivalent of the per metafield filters for use in the tsds data request later
                push(@and_fields, $meta_field_name.' '.$operator.' "'.$meta_field_value.'"');
            }
            push(@{$conditions_by_type->{$type}}, '( '.join(' and ',@and_fields).') ');
        }
    }

    # If there are conditions on the values, we can add them here to the
    # having clause. This is implicitly only valid for a single measurement
    # type which is checked above so no need to examine each type
    my $having = [];
    if ($value_field_names && @$value_field_names){
        for (my $i = 0; $i < @$value_field_names; $i++){
            my $val_field = $value_field_names->[$i];
            my $val_logic = $value_field_logics->[$i];
            my $val_val   = $value_field_values->[$i];
            my $val_func  = $value_field_functions->[$i];

            push(@$having, {field => $val_field,
                            logic => $val_logic,
                            value => $val_val,
                            function => $val_func});
        }
    }

    # query sphinx if a search term was passed in
    my $meta_ids_by_type = {};
    my ($count, $total, $warning);
    if(!$skip_sphinx_search){
        my ($sphinx_meta, $sphinx_results) = $self->_sphinx_search(
            search                  => $search,
            types                   => $types, 
            weights                 => $weights,
            limit                   => $limit,
            offset                  => $offset,
            start_epoch             => $start_epoch,
            end_epoch               => $end_epoch,
            per_field_search_string => $per_field_search_string
        );

        # grab the total matches from the sphinx meta data (coerce to integer by adding 0)
        $total = $sphinx_meta->{'total'} + 0;

        # set a warning if sphinx limited our result set
        if ($sphinx_meta->{'total_found'} > $sphinx_meta->{'total'}) {
            my $difference = $sphinx_meta->{'total_found'} - $sphinx_meta->{'total'};
            $warning = "The total search results is greater than what has been returned by ". $difference . " results.";
        }

        # group all the identifiers by type 
        foreach my $sphinx_result (@$sphinx_results) {
            my $meta_id       = $sphinx_result->{'meta_id'};
            my $measurement_type = $sphinx_result->{'measurement_type'};
            if(!defined($meta_ids_by_type->{$measurement_type})){
                $meta_ids_by_type->{$measurement_type} = $meta_id;
            }
            # concatinate the next meta_id onto our string (it's just a big json string)
            else {
                $meta_ids_by_type->{$measurement_type} .= ",$meta_id";
            }
        }
    }
    # otherwise set the meta_id values to 'ALL'
    else {
        foreach my $type (@$types){
            $meta_ids_by_type->{$type} = 'ALL'; 
        }  
    }

    # loop through are ids grouped by type hash querying the values for each along the way
    my $search_schema  = {};
    my $search_results = [];
    foreach my $type (keys %$meta_ids_by_type) {
        # grab the appropriate schema
        my ($classifier, $measurement_type) = $self->metadata()->_parse_measurement_type( $type );
        my $schema = $schemas->{$measurement_type};

        # define the graph behaviour  
        $search_schema->{$type}{'query'}{'line_graph'} = {
            meta_fields => $schema->{'meta'}{'required'},
            by          => $schema->{'meta'}{'required'}
        };

        my $meta_ids = $meta_ids_by_type->{$type};

        # get our meta_fields 
        my $meta_fields = $self->metadata()->get_meta_fields( measurement_type => $type );

        # determine what our required and classifier fields are depending on whether or not 
        # we are currently deally with a classifier
        my ($required_meta_fields, $ordinal_meta_fields);
        if(defined($classifier)){
            $ordinal_meta_fields  = $schema->{'meta'}{'classifier'}{$classifier}{'ordinal'};
            $required_meta_fields = $ordinal_meta_fields;
        } else {
            $required_meta_fields = $schema->{'meta'}{'required'};
            $ordinal_meta_fields  = $schema->{'meta'}{'ordinal'};
        }

        my $base_required_meta_fields = $schema->{'meta'}{'required'};

        # create a ordinal meta field map for sorting the meta_field results later
        my $ordinal_meta_field_map = {};
        my $order_meta_fields = ($group_by) ? $group_by : $ordinal_meta_fields;
        for(my $i = 0; $i < @$order_meta_fields; $i++){
            my $field = $order_meta_fields->[$i];
            $ordinal_meta_field_map->{$field} = ($i + 1);
        }

        # put value field info in search_schema for the application side to use
        my $ordinal_value_field_map = {};
        my $value_fields = $schema->{'value'}{'ordinal'};
        for(my $i = 0; $i < @$value_fields; $i++){
            my $value_field = $value_fields->[$i]; 
            $search_schema->{$type}{'values'}{$value_field} = $schema->{'value'}{'fields'}{$value_field};
            $ordinal_value_field_map->{$value_field} = ($i + 1);
        }

         

        # get the data results for this measurement_type
        my $data_results = $self->_get_search_result_data(
            measurement_type => $measurement_type,
            start_time => $start_time,
            end_time => $end_time,
            step => $step,
            aggregator => $aggregator,
            base_required_meta_fields => $base_required_meta_fields,
            required_meta_fields => $required_meta_fields,
            ordinal_meta_fields  => $ordinal_meta_fields,
            value_fields => $value_fields,
            meta_ids => $meta_ids,
            conditions => $conditions_by_type->{$type},
            limit => $limit,
            offset => $offset,
            total => \$total,
            order => $order,
            order_by => $order_by,
            group_by => $group_by,
            having   => $having
        ) || return;

        # loop through each of our data_results building arrays of names and values for each
        foreach my $result ( @$data_results ){
            my $row = {
                measurement_type => $type,
                names  => [],
                values => [],
                search_values => []
            };

            # loop through each of our keys in our results
            foreach my $k (keys %$result ) {
                my $v = $result->{$k};
                # if it's the measurement_type leave it as is
                next if($k eq 'measurement_type');
                
                # did we use this field in a search by value, document
                # what the return val we got for it was
                if ($k =~ /^searchvalue__(.+)/) {
                    push(@{$row->{'search_values'}}, {
                        name => $1,
                        value => $v
                         });
                    
                }
                # it's a value field
                elsif( $k =~ /(.*?)_value_(.*)/){
                    my $name = $1;
                    my $type = $2;

                    # check if we already have a value with this name
                    # stored, if so put the type underneath it
                    my $found_value = 0;
                    foreach my $value (@{$row->{'values'}}){
                        if($value->{'name'} eq $name){
                            $value->{'value'}{$type} = $v;
                            $found_value = 1;
                            last;
                        }
                    }

                    next if($found_value);

                    # otherwise add the new value to our values array
                    push(@{$row->{'values'}}, {
                        value => { $type => $v },
                        name  => $name
                    });
                    next;
                }
                else {
                    # otherwise we're a name, do the name thing
                    push(@{$row->{'names'}}, {
                        name  => $k,
                        value => $v
                         });
                }
            }
            # sort the meta field names by their ordinal values
            @{$row->{'names'}} = sort { 
                $ordinal_meta_field_map->{$a->{'name'}} <=> $ordinal_meta_field_map->{$b->{'name'}}
            } @{$row->{'names'}};
            
            # sort the value field names by their ordinal values
            @{$row->{'values'}} = sort { 
                $ordinal_value_field_map->{$a->{'name'}} <=> $ordinal_value_field_map->{$b->{'name'}}
            } @{$row->{'values'}};

            push @$search_results, $row;
        }
    }

    # get the query end time and determine the elapsed time
    my $run_end_time = Time::HiRes::gettimeofday();
    my $elapsed_time = $run_end_time - $run_start_time;

    my $results  = {
          results => $search_results,
          step => $step + 0,
          aggregator => $aggregator,
          elapsed_time => sprintf('%.3f', $elapsed_time) + 0,
          total => $total + 0,
          limit => $limit + 0,
          offset => $offset + 0,
          schema => $search_schema
    };
    $results->{'warning'} = $warning if(defined($warning));

    return $results;
}

# helper method to query the actual data values from the tsds database
sub _get_search_result_data {
    my ($self, %args) = @_;
    my $measurement_type = $args{'measurement_type'};
    my $start_time       = $args{'start_time'};
    my $end_time         = $args{'end_time'};
    my $meta_ids         = $args{'meta_ids'};
    my $step             = $args{'step'};
    my $aggregator       = $args{'aggregator'};
    my $value_fields     = $args{'value_fields'};
    my $conditions       = $args{'conditions'};
    my $limit            = $args{'limit'};
    my $offset           = $args{'offset'};
    my $total            = $args{'total'};
    my $order            = $args{'order'};
    my $order_by         = $args{'order_by'};
    my $group_by         = $args{'group_by'};
    my $having           = $args{'having'};
    my $ordinal_meta_fields  = $args{'ordinal_meta_fields'};
    my $required_meta_fields = $args{'required_meta_fields'};
    my $base_required_meta_fields = $args{'base_required_meta_fields'};
    # generate our value fields 
    my $value_agg_names = [];
    my $value_queries   = [];
    my $outer_value_queries = [];

    # build the tsds strings needed for each of our oridinal values
    foreach my $value (@$value_fields) {
        # create the series values
        my $series_field = ' aggregate(values.' . $value . ", $step, $aggregator)";
        my $values_rename = $value . '_value_series';
        my $values_query = " $series_field as $values_rename";
        push(@$value_queries, $values_query);
        push(@$outer_value_queries, $values_rename);

        # create the aggregate value
        my $value_agg_name = $value.'_value_aggregate';
        my $avg_value_query = " $aggregator(" . $series_field . ') as '. $value_agg_name;
        push(@$value_queries, $avg_value_query);
        my $outer_agg_value_query = "sum(all($value_agg_name)) as $value_agg_name";
        push(@$value_agg_names, $value_agg_name);

        push(@$outer_value_queries, $outer_agg_value_query);

        foreach my $have (@$having){
            next unless ($have->{'field'} eq $value);
            my $func = $have->{'function'};
            my $having_get = "";
            if ($func =~ /^percentile_(\d+)$/){
                $having_get = "percentile(aggregate(values.$value, $step, $aggregator), $1)";
            }
            else {
                $having_get = "$func(aggregate(values.$value, $step, $aggregator))";
            }
            my $rename = "searchvalue__$func$value";
            $have->{'named_as'} = $rename;
            $having_get .= " as $rename";
            push(@$value_queries, $having_get);
            push(@$outer_value_queries, $rename);
        }
    }

    my $meta_fields = defined($group_by) ? $group_by : $ordinal_meta_fields;
    my $inner_by_fields   = $base_required_meta_fields;
    my $outer_by_fields   = defined($group_by) ? $group_by : $required_meta_fields;


    my $wheres = [];
    # if a search term was entered apply the appropriate where clause
    if($meta_ids ne 'ALL') {
        my $where = $self->_translate_meta_ids( $meta_ids ) || return;
        push(@$wheres, $where);
    }
    # need to apply the tsds equivalent of the sphinx search query in the where clause 
    # to scope the data returned appropriately
    if(@$conditions){
        my $where = '( '.join(' and ', @$conditions).' )';
        push(@$wheres, $where);
    }

    my @having_strings;
    my $order_by_val = 0;
    if (@$having){
        $order_by_val = 1;
        foreach my $have (@$having){
            push(@having_strings, "$have->{'named_as'} $have->{'logic'} $have->{'value'}");
        }
    }
    
    # if order by fields were passed in parse them and apply them to our query
    my $ordered_by_strings = [];
    if($order_by){
        foreach my $order_by_field (@$order_by){
            if($order_by_field =~ /(name|value)_(\d+)/){
                my $field;
                # order by value
                if($1 eq 'value'){
                    $order_by_val = 1;
                    # subtract one since the webservice parameters are 1 indexed and
                    # our array is 0 indexed
                    $field = $value_agg_names->[($2 - 1)] || next;                    
                }
                # otherwise its order by name 
                else {
                    # subtract one since the webservice parameters are 1 indexed and
                    # our array is 0 indexed
                    $field = $meta_fields->[($2 - 1)] || next;
                }
                push(@$ordered_by_strings, "$field $order");
            } else {
                $self->error('order_by fields must be in format (value|name)_\d+');
                return;
            }
        }   
    }

    # we first need to do a query based on the outer by fields to figure out the first $limit / $offset
    # set of them. Since on the inner query we're doing a by on the base required fields, the limit/offset/grouping
    # doesn't actually work as expected since it'll find the first $limit say interfaces, not pops.
    # This part is aimed to find the first $limit pops and amend the where clause to reflect that.
    # This whole part isn't relevant if ordering by value
    if (! $order_by_val){
        log_debug("Performing inner search");

        my $where_query = "get ";
        $where_query   .= join(", ", @$outer_by_fields);
        $where_query   .= " between(\"$start_time\", \"$end_time\") ";
        $where_query   .= " by ";
        $where_query   .= join(", ", @$outer_by_fields);
        $where_query   .= " from $measurement_type ";
        $where_query   .= ' where '.join(' and ', @$wheres). ' ' if(@$wheres);
        $where_query   .= " limit $limit offset $offset " if($meta_ids eq 'ALL');
        $where_query   .= " ordered by ".join(', ', @$ordered_by_strings).' ' if (@$ordered_by_strings);
        
        my $where_results = $self->parser()->evaluate($where_query, force_constraint => 1);
        if (!$where_results) {
            $self->error( "Error parsing pre-search data where results: ".$self->parser()->error());
            return;
        }
        
        # if we don't have a search term we need to calculate our total here 
        # instead of relying on sphinx to return it.
        # Since this was the query that determined the later "by" clause it actually
        # has the total we want to expose upwards, not the queries below
        ${$total} += $self->parser()->total() if($meta_ids eq 'ALL');

        my @additional_wheres;
        foreach my $where_result (@$where_results){
            my @arr;
            foreach my $key (keys %$where_result){
                if (defined $where_result->{$key}){
                    push(@arr, " $key = \"" . $where_result->{$key} . "\" ");
                }
                else {
                    push(@arr, " $key = null");
                }
            }
            my $str = "(" . join(" and ", @arr) . ")";
            push(@additional_wheres, $str);
            
        }
        
        if (! @additional_wheres){
            log_debug("Aborting inner search early due to no where bits found");
            return [];
        }

        push(@$wheres, join(" or ", @additional_wheres));
    }

    # create our tsds query
    my $inner_query = 'get ';
    $inner_query .= join(', ', @$meta_fields) . ', ';
    $inner_query .= join(', ', @$value_queries);
    $inner_query .= ' between  ("'.$start_time.'","'.$end_time.'") ';
    $inner_query .= ' by ';
    $inner_query .=  join(', ', @$inner_by_fields);
    $inner_query .= " from $measurement_type ";
    $inner_query .= ' where '.join(' and ', @$wheres). ' ' if(@$wheres);
    $inner_query .= " having " . join(' and ', @having_strings) if (@having_strings);
    $inner_query .= " ordered by ".join(', ', @$ordered_by_strings).' ' if (@$ordered_by_strings);

    my $query = 'get ';
    $query  .= join(', ', @$meta_fields) . ', ';
    $query  .= join(', ', @$outer_value_queries);
    $query  .= " by " ;
    $query  .= join(', ', @$outer_by_fields);
    $query  .= " from ( $inner_query ) ";

    if ($order_by_val && $meta_ids eq 'ALL'){
        $query   .= " limit $limit offset $offset ";
    }

    $query  .= " ordered by ".join(', ', @$ordered_by_strings).' ' if (@$ordered_by_strings);

    # log the query when in debug mode
    log_debug("tsds query: '$query'");


    # execute our query
    my $data_results = $self->parser()->evaluate($query, force_constraint => 0);
    if (!$data_results) {
        $self->error( "Error parsing search data results: ".$self->parser()->error());
        return;
    }

    # if we don't have a search term we need to calculate our total here 
    # instead of relying on sphinx to return it.
    # If we did NOT order by value then we will have already grabbed
    # the total above. If we DID order by value we need to do it here
    if ($order_by_val){
        ${$total} += $self->parser()->total() if($meta_ids eq 'ALL');
    }

    return $data_results;
}

# translates a json string containing an array of objects into tsds query language
# where clause conditions
sub _translate_meta_ids {
    my ($self, $meta_ids) = @_;

    # convert the json string to a perl structure
    my $meta_ids_array;
    eval {
        $meta_ids_array = decode_json( '['.$meta_ids.']' );
    };
    if($@){
        $self->error("Problem decoding json file: $@");
        return;
    }

    # parse each element in the json structure converting it into a tsds
    # where clause condition
    my $tsdsql_strs = [];
    foreach my $meta_id (@{$meta_ids_array}){
        my $meta_id_strs = [];
        foreach my $key_value_pair (@$meta_id){
            my $field = (keys(%$key_value_pair))[0];
            my $value = $key_value_pair->{$field};

            push(@$meta_id_strs, $field.' = "'.$value.'" ');
        } 
        push(@$tsdsql_strs, '( '.join(' and ', @$meta_id_strs).' )');
    }

    return "( ".join(' or ', @$tsdsql_strs)." )";
}

# helper method that constructs a sphinxql query and issues the query to sphinx
sub _sphinx_search {
    my ( $self, %args ) = @_;
    my $search             = $args{'search'};
    my $types              = $args{'types'};
    my $weights            = $args{'weights'};
    my $start_epoch        = $args{'start_epoch'};
    my $end_epoch          = $args{'end_epoch'};
    my $limit              = $args{'limit'};
    my $offset             = $args{'offset'};

    my $per_field_search_string = $args{'per_field_search_string'} || '';

    # treat spaces as search term delimiters and replace them with ' & '
    my $formatted_search = ''; 
    if($search){
        $formatted_search = join (' & ', split(' ', $search));

        # if no '*'s are present in the search term, do the same as above but wrap each search term in '*'s
        # and OR ( | ) this with the formatted_search string above
        $formatted_search .= ' | '.join (' & ',map { "*$_*" } split(' ', $search) ) if($search !~ /\*/);

        # ISSUE=9962 remove . characters from the search string
        $search =~ s/\.//g;
    }   

    # properly quote the search term
    $formatted_search =~ s/\//\\\\\//g;
    $formatted_search =~ s/@/\\\\@/g;
    $formatted_search =~ s/-/\\\\-/g;

    # build our weights array
    my @weights;
    foreach my $field (keys %$weights){
        my $weight = $weights->{$field};
        $field =~ s/\./_/g;
        push(@weights, "$field=$weight");
    }

    # 
    #  (╯°□°）╯︵ ┻━┻
    # 
    # Our date filter brought to you by sphinxQL
    # using 'IF', '+', and '*' to implement 'AND' and 'OR'
    # logic b/c making 'OR' available in the WHERE clause of the
    # query would have been too easy...
    #
    # * == AND
    # + == OR
    # end=0 means end is null (active)
                      # measurement start is between passed in start and end
    my $date_filter = "( ( IF(start <= $end_epoch,   1, 0) * ".
                      "    IF(start >= $start_epoch, 1, 0) ) ".
                      # OR
                      " + ".
                      # measurement end is between passed in start and end
                      "( ( IF( end >= $start_epoch, 1, 0) + IF( end = 0, 1, 0) ) ".
                      " * ".
                      " IF( end <= $end_epoch, 1, 0) ) ) as datefilter";

    # measurement type filter is similar to the date filter, basically use an 'SELECT IF' to return a 1
    # when it matches and a zero if it doesn't and sum the results foreach of our measurement types.
    # if any of the measurements matched this row the sum will be >= 1
    my $measurement_type_filter = join('+ ', map { qq/ IF( measurement_type = '$_', 1, 0) / } @$types);
    $measurement_type_filter    = "($measurement_type_filter) as measurement_type_filter";

    # build our sphinxql query
    my $query = "SELECT *, ".
                "WEIGHT() as weight, ".
                "$date_filter, ".
                "$measurement_type_filter ".
                'FROM tsds_metadata_index, tsds_metadata_delta_index WHERE ';

    $query .= "MATCH( '$formatted_search $per_field_search_string' ) AND ";

    #  (╯°□°）╯︵ ┻━┻  don't forget to order by your measurement_type_filter and your date_filter b/c the 
    #  expression in your where clause will only apply to the first MAX_SEARCH_RESULTS matches b/c sphinx. and you
    #  could potentially omit matches
    $query .= "datefilter > 0 AND measurement_type_filter > 0 ".
                "GROUP BY meta_id ".
                "ORDER BY measurement_type_filter desc, ".
                "  weight desc ";
    # do not want to apply limit/offset if we're ordering by a name or value
    if(defined($offset) && defined($limit)){
        $query .=   "LIMIT $offset, $limit "; 
    } 
    # have to send a limit and offset when one is not set otherwise sphinx silently limits you 
    # to 20 results 
    else {
        $query .=   'LIMIT 0, '.MAX_SEARCH_RESULTS.' '; 
    }
    $query .=   "OPTION ".
                " ranker=".SPH_RANK_SPH04.", ".
                ' field_weights=('.join(', ',@weights).'), '.
                ' max_matches=' . MAX_SEARCH_RESULTS;

    log_debug("sphinx query: '$query'");

    # execute the sphinxql query and push the matches onto our matches array
    my $sth = $self->dbh()->prepare($query);

    $sth->execute();
    my $matches = [];
    while (my $row = $sth->fetchrow_hashref) {

        push(@$matches, $row);
    }

    # need to call sphinxQLs 'SHOW META' command to get the total matches from the last query
    # for pagination
    $sth = $self->dbh()->prepare("SHOW META");
    $sth->execute();
    my $meta = {};
    while (my $row = $sth->fetchrow_hashref) {
        my $key   = $row->{'Variable_name'};
        my $value = $row->{'Value'};
        $meta->{$key} = $value;
    }

    return ($meta, $matches);
}

# helper function that converts the arrays of meta_field names, values, and logic operators into
# a sphinxql string
sub _create_per_field_search {
    my ($self, %args) = @_;

    my $measurement_types = $args{'measurement_types'};
    my $meta_field_names  = $args{'meta_field_names'};
    my $meta_field_values = $args{'meta_field_values'};
    my $meta_field_logics = $args{'meta_field_logics'};

    # convert measurement_types and meta_field_names '.'s to '_'s
    my @measurement_types_underscore = map { $_ =~ s/\./_/g; $_; } @{dclone($measurement_types)};
    my @meta_field_names_underscore  = map { $_ =~ s/\./_/g; $_; } @{dclone($meta_field_names)};

    # create a hash of all the fields we are indexing in sphinx
    my $all_fields = {};
    
    # get fields from main index;
    my $query  = "desc tsds_metadata_index";
    my $sth = $self->dbh()->prepare($query);
    $sth->execute();
    while (my $column = $sth->fetchrow_hashref) {
        $all_fields->{$column->{'Field'}} = $column;
    }

    # get fields from delta index;
    $query  = "desc tsds_metadata_delta_index";
    $sth = $self->dbh()->prepare($query);
    $sth->execute();
    while (my $column = $sth->fetchrow_hashref) {
        $all_fields->{$column->{'Field'}} = $column;
    }

    # now create a map from the user passed in field names to the user 
    # defined logic and values passed in
    my $field_map = {};
    for( my $i = 0; $i < @meta_field_names_underscore; $i++ ){
        my $name  = $meta_field_names_underscore[$i];
        my $value = $meta_field_values->[$i];
        my $logic = $meta_field_logics->[$i];

        $field_map->{$name} = {
            value => $value,
            logic => $logic
        };
    }

    # now figure our all the full index names we need to match on given our passed in match_fields
    # and measurement_types. for instance is the user passed in 'intf' for a meta_field create a shinxql 
    # match string for every passed in measurement_type that has 'intf' as a meta_field
    my $sphinx_per_field_searches = [];
    foreach my $full_field_name (keys %$all_fields) {
        my $column = $all_fields->{$full_field_name};
        if(($column->{'Type'} eq 'field') && ($column->{'Field'} =~ /(.*)__(.*)/)){
            my $measurement_type = $1;
            my $field            = $2;

            # check if the field / measurement_type is one the user has passed in
            # and if so add it to our list of sphinx per field queries
            if( (grep { $measurement_type eq $_ } @measurement_types_underscore) &&
                (grep { $field eq $_ } @meta_field_names_underscore) ){
                my $value = $field_map->{$field}{'value'};
                my $logic = $field_map->{$field}{'logic'};

                my $operator = '';
                if($logic eq 'is'){
                    $operator = '=';
                }
                elsif($logic eq 'is_not'){
                    $operator = '=-';
                }
                elsif($logic eq 'contains'){
                    $operator = '';
                }
                elsif($logic eq 'does_not_contain'){
                    $operator = '-';
                }
                else {
                    $self->error("Do not know how to hanld logic, $logic");
                    return;
                }
               
                my $sphinx_search_term; 
                # for exact matches we need to include the start (^) and end ($) string
                # characters otherwise we will match things that do not exactly match
                if($logic eq 'is' || $logic eq 'is_not'){
                    $sphinx_search_term = '"^'.$value.'$"';
                }else {
                    $sphinx_search_term = '"'.$value.'"';
                }
                push(@$sphinx_per_field_searches, "@($full_field_name) ".$operator.$sphinx_search_term);
            }
        }
    }

    return join(' ', @$sphinx_per_field_searches);
}

sub dbh {

    my ( $self, $dbh ) = @_;

    $self->{'dbh'} = $dbh if ( defined( $dbh ) );

    return $self->{'dbh'};
}

sub metadata {

    my ( $self, $metadata ) = @_;

    $self->{'metadata'} = $metadata if ( defined( $metadata ) );

    return $self->{'metadata'};
}

1;
