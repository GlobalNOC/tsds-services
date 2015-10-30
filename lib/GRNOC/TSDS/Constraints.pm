package GRNOC::TSDS::Constraints;

use Moo;

use GRNOC::Config;

use JSON::XS;
use Data::Dumper;

### required attributes ###

has config_file => ( is => 'ro',
                     required => 1 );

### private attributes ###

has config => ( is => 'rwp' );

has json => ( is => 'ro',
              default => sub { JSON::XS->new() } );

### constructor builder ###

sub BUILD {

    my ( $self ) = @_;

    # create and store config object
    my $config = GRNOC::Config->new( config_file => $self->config_file,
                                     force_array => 0 );

    $self->_set_config( $config );

    return $self;
}

### public methods ###

sub parse_constraints {

    my ( $self, %args ) = @_;

    my $database = $args{'database'};

    die( "database required" ) if ( !defined $database );

    my $constraints = $self->get_constraints();

    # no constraints found
    return if !$constraints;

    my $queries = [];

    foreach my $constraint ( @$constraints ) {

        # this constraint is for a different database than we're interested in
        next if ( $constraint->{'database'} ne $database );

        my $metadatas = $constraint->{'metadata'};

        foreach my $metadata ( @$metadatas ) {

            # raw query?
            if ( defined $metadata->{'query'} ) {

                my $query = $self->json->decode( $metadata->{'query'} );

                push( @$queries, $query );
            }

            # regular expression?
            elsif ( defined $metadata->{'regex'} ) {

                my $field = $metadata->{'field'};
                my $regex = $metadata->{'regex'};

                my $query = {$field => {'$regex' => $regex}};

                push( @$queries, $query );
            }

            # value match
            else {

                my $field = $metadata->{'field'};
                my $value = $metadata->{'value'};

                my $query = {$field => $value};

                push( @$queries, $query );
            }
        }
    }

    return {'$and' => $queries};
}

sub get_constraints {
    my ( $self, %args ) = @_;

    $self->config->{'force_array'} = 1;
    my $constraints = $self->config->get( '/config/constraint' );
    $self->config->{'force_array'} = 0;

    return $constraints;
}

1;
