package GRNOC::TSDS::SearchIndexer;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';
use lib './venv/lib/perl5';

# marpa complains unless we load this before everything else...
use GRNOC::TSDS::Parser;

use Moo;

use GRNOC::CLI;
use GRNOC::Log;
use GRNOC::TSDS::MongoDB;
use GRNOC::TSDS::Constants;
use GRNOC::TSDS::DataService::MetaData;

use JSON qw( encode_json );
use Template;
use POSIX qw( ceil );
use Storable qw( dclone );
use Data::Dumper;
use Time::HiRes qw( time );

use constant XMLPIPE2_HEADER        => '<?xml version="1.0" encoding="utf-8"?>'."\n".'<sphinx:docset>';
use constant XMLPIPE2_SCHEMA_TMPL   => 'xmlpipe2_schema.xml';
use constant XMLPIPE2_DOCUMENT_TMPL => 'xmlpipe2_document.xml';
use constant XMLPIPE2_FOOTER        => '</sphinx:docset>';
use constant XMLPIPE2_SCHEMA_ATTRS  => [
    { name => 'start', type => 'timestamp' }, 
    { name => 'end', type => 'timestamp' }, 
    { name => 'last_updated', type => 'timestamp' }, 
    { name => 'identifier', type => 'string' },
    { name => 'measurement_type', type => 'string' },
    { name => 'meta_id', type => 'string' }
];
# don't index these fields when set on a measurement
use constant IGNORED_FIELDS => [ '_id', 'values' ];

### required attributes ###

has config_file => ( 
    is => 'ro',
    required => 1 
);

has sphinx_templates_dir => (
    is => 'ro',
    required => 1
);

### optional attributes ###

has num_docs_per_fetch => (
    is => 'ro',
    default => 1000
);

has quiet => ( 
    is => 'ro',
    default => 0
);

has pretend => ( 
    is => 'ro',
    default => 0 
);

has last_updated_offset => (
    is => 'ro',
    default => 0
);

### internal attributes ###

has metadata => ( is => 'rwp' );

has mongo_ro => ( is => 'rwp' );

has tt       => ( is => 'rwp' );

has sphinx_id => ( 
    is       => 'rwp',
    default  => 1
);

### constructor builder ###

sub BUILD {

    my ( $self ) = @_;

    # create template toolkit for our xmlpipe2 templates
    my $tt = Template->new(
        INCLUDE_PATH => $self->sphinx_templates_dir()
    );
    if(!$tt){
        die "Error creating GRNOC::TSDS::SearchIndexer, template toolkit: ".$tt->error();
    }
    $self->_set_tt($tt);

    # create our metadata webservice 
    my $metadata = GRNOC::TSDS::DataService::MetaData->new(
        config_file => $self->{'config_file'} 
    ) || die "Couldn't instantiate the MetaData DataService";
    $self->_set_metadata( $metadata );

    # connect to mongo
    $self->_mongo_connect();

    return $self;
}

### public methods ###

sub index_metadata {

    my ( $self ) = @_;

    # ISSUE=12500 properly handle unicode characters
    binmode( STDOUT, ":utf8" );

    my $start_time = time;
    log_info('starting sphinx xmlpipe2 generation...');
    
    # start the xmlpipe2 doc 
    print XMLPIPE2_HEADER . "\n"; 

    # loop through each of our measurement_types creating the fields and attributes needed
    # for our sphinx schema
    my $schema_fields = [];
    my $schemas = $self->metadata()->get_measurement_type_schemas();
    foreach my $measurement_type (keys %$schemas){
        my $schema = $schemas->{$measurement_type};
        my $meta_fields = $self->mongo_ro()->flatten_meta_fields(
            prefix => '',
            data   => $schema->{'meta'}{'fields'}
        );

        push(@$schema_fields, map { $measurement_type.'__'.$_ } keys(%$meta_fields));
        foreach my $classifier (keys %{$schema->{'meta'}{'classifier'}}){
            push(@$schema_fields, map { "$measurement_type.$classifier".'__'.$_ } keys(%$meta_fields));
        }
    }

    # sphinx doesn't like '.'s in it's field so make those underscores
    map { s/\./_/g } @$schema_fields; 

    # generate the sphinx xmlpipe2 schema and print it
    my $vars = {
        fields => $schema_fields,
        attrs  => XMLPIPE2_SCHEMA_ATTRS
    };
    my $xmlpipe2_schema;
    if(!$self->tt()->process( XMLPIPE2_SCHEMA_TMPL, $vars, \$xmlpipe2_schema)){
        log_error('Error processing, '.XMLPIPE2_SCHEMA_TMPL.' with vars '._pp($vars).': '.$self->tt()->error());
        return;
    }
    # print out our sphinx xmlpipe2 document
    print "$xmlpipe2_schema\n";
   
    # now loop through each of our measurement_types again indexing each measurements meta data 
    my $count = 1;
    foreach my $measurement_type (keys %$schemas){

        # log a status message
        log_debug( "$measurement_type ($count/".keys(%$schemas).")" );

        # index all of the metadata in this database 
        $self->_index_metadata( 
            schema => $schemas->{$measurement_type},
            measurement_type => $measurement_type 
        );
        $count++;
    }
    print XMLPIPE2_FOOTER . "\n";

    my $end_time = time;
    my $elapsed_time = sprintf( '%.3f', ($end_time - $start_time) );

    log_info('finished sphinx xmlpipe2 generation... elapsed time: ('.$elapsed_time.' seconds)');

    return 1;
}

### private methods ###
sub _index_metadata {
    my ( $self, %args )  = @_;
    my $schema           = $args{'schema'};
    my $measurement_type = $args{'measurement_type'};

    # get ahold of this mongo database instance
    my $database = $self->mongo_ro()->get_database( $measurement_type );

    my $required_fields = $schema->{'meta'}{'required'};
    my $classifiers     = $schema->{'meta'}{'classifier'};

    my $msmt_col = $database->get_collection( 'measurements' );

    my $start_time = time - ($self->last_updated_offset*60) if(defined($self->last_updated_offset) and 
                                                              ($self->last_updated_offset > 0));
    my $total      = $msmt_col->count();
    my $limit      = $self->num_docs_per_fetch; 
    $limit = $total if($total < $limit); 

    # index $limit measurements at a time
    for(my $offset = 0; $offset < $total; $offset += $limit){
    
	log_debug("creating sphinx xmlpipe2 documents for (limit: $limit, offset: $offset) of $total...");

        my $find = { end => undef };
        # if last_updated_offset is specified, use it to get only measurements updated in the past specified days
        if (defined($start_time)) {
	    $find = { last_updated => { '$gte' => $start_time } };
        }
        my @msmts = $msmt_col->find($find)->limit($limit)->skip($offset)->all();

        # flatten out measurements to be indexed and make separate indexes for any classifiers that are set.
        # ISSUE=892 PROJ=160: Write out the xmlpipe2 docs as we go to avoid problems with excessively large 
        # arrays/memory use in certain situations.
        $self->_format_and_write_measurements(
            database_name   => $database->{'name'}, 
            measurements    => \@msmts,
            classifiers     => $classifiers,
            required_fields => $required_fields
        );

    }
        
    return 1;
}


# takes a flattened measurement and write out an xmlpipe2 doc 
sub _write_doc  {
    my ($self, $msmt) = @_;

    # set our main index for this measurement
    my $index = {
        id      => $self->sphinx_id,
        fields  => $msmt
    };

    # sphinx xml2pipe formatted json documents for our index
    my $xmlpipe2_doc;
    if(!$self->tt()->process( XMLPIPE2_DOCUMENT_TMPL, $index, \$xmlpipe2_doc)){
        log_error('Error processing, '.XMLPIPE2_DOCUMENT_TMPL.' with vars '._pp($index).': '.$self->tt()->error());
        return;
    }
    $self->_set_sphinx_id( $self->sphinx_id + 1 );

    # print out our sphinx xmlpipe2 document
    print "$xmlpipe2_doc\n"; 

}


# takes a perl hash and dot notation string.
# splits the dot notation string and makes sure each string
# element is defined in the hash at each level and if so returns the last value
# i.e. msmt = { circuit => { name => 'I2-CKT-001' } }, field = 'circuit.name'
# would return 'I2-CKT-001
sub _get_dot_notation_field {
    my ($self, $msmt, $field) = @_;

    my $current_value = dclone($msmt);

    my @fields = split('\.', $field);
    foreach my $field (@fields){
        # if we hit a scalar value before we've reached the end of our dot notation
        # there was a problem in the way the data was stored. for example, we expected a 
        # hash but whatever entered the metadata sent it as the perl string 
        # 'HASH(0x675de20)'
        if(ref(\$current_value) eq 'SCALAR'){
            log_error("Hit unexpected meta data format, '$current_value', when looking for $field on measurement ".$msmt->{'identifier'}.", giving up trying to index classifier...");
            return;
        }
        $current_value = $current_value->{$field};

        return if(!defined($current_value) || $current_value eq '');
    }

    return $current_value;
}

# takes a hierarchal array of measurements hashes, flattens them, prefixes each with the measurement_type
# and unwinds any classifiers out into there own indexes if need be, writing each out as it is done.
sub _format_and_write_measurements {
    my ($self, %args)   = @_;
    my $database_name   = $args{'database_name'};
    my $msmts           = $args{'measurements'};
    my $classifiers     = $args{'classifiers'};
    my $required_fields = $args{'required_fields'};

    # loop through each measurement
    foreach my $msmt (@$msmts) {

        # add the measurement_type field to the measurement
        $msmt->{'measurement_type'} = $database_name;

        ## HANDLE CLASSIFIER MEAUREMENTS

        # check to see if there are any classifier fields set on this measurement and if so 
        # flatten and write out the docs
        my $classifier_msmts = $self->_unwind_and_write_classifiers(
            measurement => $msmt,
            classifiers => $classifiers
        );
        
        ## HANDLE MAIN MEASURMENT

        # store the meta_id on the main measurement (array of the required field'ds keys and value) used
        # for unique metadata based identification
        my $meta_id = [];
        foreach my $required_field (@$required_fields){
            my $required_value = $msmt->{$required_field};
            push(@$meta_id, {
                $required_field => $required_value
            });
        }

        # create json meta id
        my $meta_id_json;
        eval {
            $meta_id_json = encode_json( $meta_id );
        };
        if($@){
            log_error("Could not json encode meta_id, ".$self->_pp($meta_id).", for measurement, skipping: $@");
            next;
        }
        $msmt->{'meta_id'} = $meta_id_json;

        # flatten the main measurement 
        my $flat_msmt = {};
        $self->_flatten_measurement(
            ref       => $msmt,
            prefix    => $msmt->{'measurement_type'}.'_',
            flat_hash => $flat_msmt
        );

        # write out the measurement/doc
        $self->_write_doc( $flat_msmt );

    }

}

# checks a measurements for classifiers and flattens and writes out a doc for any that it does find
sub _unwind_and_write_classifiers {
    my ($self, %args) = @_;
    my $msmt        = $args{'measurement'};
    my $classifiers = $args{'classifiers'};

    # if this measurement_type has classifiers and this measurement has the classifier fields
    # set/defined, create separate indexes for the classifier fields

    foreach my $classifier (keys %$classifiers) {
        # skip if the classifier field isn't set on this measurement
        next if(!defined($msmt->{$classifier}));
        # if it's an empty hash skip it
        next if(ref($msmt->{$classifier}) eq 'HASH'  && !%{$msmt->{$classifier}});
        # if it's an empty array skip it
        next if(ref($msmt->{$classifier}) eq 'ARRAY' && !@{$msmt->{$classifier}});


        # if this classifer is not an array reference just make it an array reference
        # so we have one code path below
        my $msmt_classifiers;
        my $msmt_classifier = $msmt->{$classifier};
        if(ref($msmt_classifier) ne 'ARRAY'){
            if(ref($msmt->{$classifier}) eq 'HASH'){
                $msmt_classifiers = [dclone($msmt->{$classifier})];
            }else {
                $msmt_classifiers = [$msmt->{$classifier}];
            }
        }else {
            $msmt_classifiers = dclone($msmt->{$classifier});
        }
       
        # loop through each classifier measurement setting its meta_id and overwriting its 
        # measurement_type with the classifier field
        foreach my $msmt_classifier (@$msmt_classifiers){
            my $classifier_meta_id = [];
            # if the msmt_classifier is a scalar then make the key the classifier field and move on
            if(ref(\$msmt_classifier) eq 'SCALAR'){ 
                push(@$classifier_meta_id, { $classifier => $msmt_classifier });
            } 
            elsif(ref($msmt_classifier) eq 'HASH') {
                # grab all the oridinal fields for the classifier
                foreach my $full_ordinal_field (@{$classifiers->{$classifier}{'ordinal'}}){
                    # if this is a dot notation ordinal field remove the first key since we've already 
                    # accessed the first key above
                    my $ordinal_field = ($full_ordinal_field =~ /.*?\.(.*)/) ? $1 : $full_ordinal_field;

                    # grab the ordinal value and skip this measurement if its not defiend
                    my $ordinal_value = $self->_get_dot_notation_field(
                        $msmt_classifier,
                        $ordinal_field
                    );
                    next if(!$ordinal_value);
                    push(@$classifier_meta_id, {
                        $full_ordinal_field => $ordinal_value
                    });
                }
            }
            else {
                $self->error("Do not know how to handle values, ".$self->_pp($msmt_classifier).", underneath key $classifier, skipping...");
                next;
            } 

            # if we found ordinal fields for the classifier that were set,
            # "add this measurement to our unwound classifier list"
            if(@$classifier_meta_id){
                my $classifier_msmt = dclone($msmt);
                # create json meta id
                my $classifier_meta_id_json;
                eval {
                    $classifier_meta_id_json = encode_json( $classifier_meta_id );
                };
                if($@){
                    log_error("Could not json encode classifier_meta_id, ".$self->_pp($classifier_meta_id).", for measurement, skipping: $@");
                    next;
                }
                # change the measurement_type to be the classifer
                $classifier_msmt->{'measurement_type'} .= '.'.$classifier;
                $classifier_msmt->{'meta_id'} = $classifier_meta_id_json;

                # flatten it
                my $flat_classifier_msmt = {};
                (my $prefix = $classifier_msmt->{'measurement_type'}) =~ s/\./_/g;
                $self->_flatten_measurement(
                        ref       => $classifier_msmt,
                        prefix    => $prefix.'_',
                        flat_hash => $flat_classifier_msmt
                    );
                # and write it out 
                $self->_write_doc( $flat_classifier_msmt );
            }

        } # end loop over msmt_classifiers 
    } # end loop over classifiers

}

# Flattens a measurement with arbitrary meta data complexity and prefixes it with $measurement_type.'__'. 
# For example, the following... (assume prefix is 'interface_')
#
#   { 
#       intf    => 'xe-0/0/0'
#       circuit => [{
#           name => 'ILIGHT-0000',
#           type => '100GE'
#       },{
#            name => 'ILIGHT-0001',
#            type => 'VLAN'
#       }]
#       pop => {
#           name => 'ICONA-POP'
#       }
#   }
#
# becomes...
#
#   {
#       interface__intf => 'xe-0/0/0'
#       interface__circuit_name => 'ILIGHT-0000,ILIGHT-0001',
#       interface__circuit_type => '100GE,VLAN',
#       interface__pop_name     => 'ICONA-POP'
#   }
#
sub _flatten_measurement {
    my ($self, %args) = @_;
    my $ref       = $args{'ref'};
    my $flat_hash = $args{'flat_hash'};
    my $prefix    = $args{'prefix'};

    if(ref($ref) eq 'HASH'){
        foreach my $key (keys(%$ref)){
            # don't index ignored measurement fields
            next if(grep { $_ eq $key } @{&IGNORED_FIELDS});
                
            my $value = $ref->{$key};

            # if the key is one of our attributes just set its value on our flat hash and move on
            if( (grep { $_->{'name'} eq $key } @{&XMLPIPE2_SCHEMA_ATTRS}) ){
                $flat_hash->{$key} = $value;
                next;
            }
            
            # get rid of '.'s b/c mongo doesn't likey
            $key =~ s/\./_/g;

            # combine the prefix to our key to get the full field name
            my $full_key = (defined($prefix)) ? $prefix.'_'.$key : $key;
        
            # recursively handle other ARRAY and HASH references
            if(ref($value) eq 'ARRAY' || ref($value) eq 'HASH'){
                $self->_flatten_measurement(
                    ref       => $value,
                    prefix    => $full_key,
                    flat_hash => $flat_hash
                );
            }
            # otherwise set the scalar value on this key in our flat_hash.
            # if this key already has a defined value this key represents a field that
            # is an array. append the new value onto the current value prefixed with a comma
            else {
                # if we haven't seen this value yet
                if(!defined($flat_hash->{$full_key})){
                    $flat_hash->{$full_key} = $value; 
                }
                # if this key already has a defined value this key represents a field that
                # is an array. append the new value onto the current value prefixed with a comma
                elsif(defined($value)) {
                    $flat_hash->{$full_key} .= ",$value";
                }
            }
        }
    }
    elsif(ref($ref) eq 'ARRAY'){
        # if we were given an array reference loop through each element
        foreach my $element (@$ref){
            # recursively handle elements that are array or hash references
            if(ref($element) eq 'ARRAY' || ref($element) eq 'HASH'){
                $self->_flatten_measurement(
                    ref       => $element,
                    prefix    => $prefix,
                    flat_hash => $flat_hash
                );
            }
            # otherwise set the scalar value on this key in our flat_hash.
            # if this key already has a defined value this key represents a field that
            # is an array. append the new value onto the current value prefixed with a comma
            else {
                # if we haven't seen this value yet
                if(!defined($flat_hash->{$prefix})){
                    $flat_hash->{$prefix} = $element; 
                }
                # if this key already has a defined value this key represents a field that
                # is an array. append the new value onto the current value prefixed with a comma
                elsif(defined($element)) {
                    $flat_hash->{$prefix} .= ",$element";
                }
            }
        }
    }
    # ref should always be either a HASH or an ARRAY ref, if not somethings' wrong
    else {
        $self->error("Arg ref should be either a HASH or an ARRAY ref in flatten_measurement method");
        return;
    }

    return 1;
}

# helper method to pretty print perl objects
sub _pp {
   my $obj = shift;

   my $dd = Data::Dumper->new([$obj]);
   $dd->Terse(1)->Indent(0);

   return $dd->Dump();
}

# helper method to connect to mongo
sub _mongo_connect {
    my ( $self ) = @_;

    my $mongo = GRNOC::TSDS::MongoDB->new( 
        config_file => $self->config_file,
        privilege   => 'ro'
    );

    $self->_set_mongo_ro( $mongo );
}

1;
