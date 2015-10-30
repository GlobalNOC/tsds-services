package GRNOC::TSDS::Upgrade;

use Moo;

use GRNOC::TSDS;
use GRNOC::CLI;
use GRNOC::Config;

use GRNOC::TSDS::Install;

use Data::Dumper;
use MongoDB;
use Sort::Versions;

### required attributes ###

has config_file => ( is => 'ro',
                     required => 1 );

### internal attributes ###

has cli => ( is => 'ro',
             default => sub { return GRNOC::CLI->new() } );

has config => ( is => 'rwp' );

has mongo_root => ( is => 'rwp' );

has error => ( is => 'rwp' );

### constructor builder ###

sub BUILD {

    my ( $self ) = @_;

    my $config = GRNOC::Config->new( config_file => $self->config_file,
                                     force_array => 0 );

    $self->_set_config( $config );

    my $mongo_host = $self->config->get( '/config/mongo/@host' );
    my $mongo_port = $self->config->get( '/config/mongo/@port' );
    my $root_user  = $self->config->get( "/config/mongo/root" );

    my $mongo_root;
    eval {
        $mongo_root = MongoDB::MongoClient->new(
            host => "$mongo_host:$mongo_port",
            query_timeout => -1,
            username => $root_user->{'user'},
            password => $root_user->{'password'}
            );
    };
    if($@){
        die "Could not connect to Mongo: $@";
    }

    $self->_set_mongo_root( $mongo_root );
}

### public methods ###

sub upgrade {

    my ( $self ) = @_;

    $self->cli->clear_screen();
    $self->_print_banner();

    my $old_version = $self->_get_old_version();
    print "The schema is currently running version $old_version.\n\n";

    # get all of the upgrades they will need to run
    my @upgrades = $self->_get_upgrades();

    # there aren't any more upgrades to perform
    if ( @upgrades == 0 ) {

        print "No more upgrades are required.\n";
        return 1;
    }

    print "The following upgrades are required, listed in the correct order: \n\n";

    foreach my $upgrade ( @upgrades ) {

        print "$upgrade\n";
    }

    print "\n";
    
    my $sure = $self->cli->get_input( 'Are you sure you want to perform these upgrades? [y/N]',
                                      default => 'N',
                                      required => 0,
                                      pattern => 'y|N' );
    
    # they weren't sure
    return if ( $sure =~ /n/i );
    
    # dynamically load and execute each necessary upgrade module
    foreach my $upgrade ( @upgrades ) {
	
        print "Upgrading from $old_version to $upgrade...\n";
	
        my $update = {'$set' => {'version' => $upgrade}};
	
        # replace periods with underscores
        $upgrade =~ s/\./_/g;
	
        # determine both the module name and the path/require name
        my $module_name = "GRNOC::TSDS::Upgrade::" . $upgrade;
        my $require_name = "GRNOC/TSDS/Upgrade/$upgrade" . ".pm";
	
        # import the module
        require $require_name;

	# make sure the expected previous version matches
        if ( defined( $module_name->PREVIOUS_VERSION() ) && $old_version ne $module_name->PREVIOUS_VERSION() ) {
	    
            $self->_set_error( "Expected version " . $module_name->PREVIOUS_VERSION() . " but the schema is currently running $old_version." );
            return;
        }
	
        my $ret;
	
        # run its upgrade method, passing along a reference to this upgrade object
        $module_name->upgrade( $self );
	
        # bump up the version in the schema
        $self->mongo_root->get_database( 'tsds_version' )->get_collection( 'tsds_version' )->update( {}, $update );
	
        # store this new version #
        $old_version = $self->_get_old_version();
    }
    
    return 1;
}

### private methods ###

sub _print_banner {

    my ( $self ) = @_;

    # what version of CDS2 is this
    my $version = $GRNOC::TSDS::VERSION;

    # whats the current year
    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime();
    $year += 1900;

    # print the banner title
    $self->_print_title( "Global Research NOC TSDS Upgrader v$version - Copyright(C) $year The Trustees of Indiana University" );
}

sub _print_title {

    my ( $self, $title ) = @_;

    my $len = length( $title );

    print "$title\n";

    # print some dash characters underneath the title
    print '=' x $len, "\n\n";
}

sub _get_old_version {

    my ( $self ) = @_;

    my $old_version = $self->mongo_root->get_database( 'tsds_version' )->get_collection( 'tsds_version' )->find_one()->{'version'};

    return $old_version;
}

sub _get_upgrades {

    my ( $self ) = @_;

    my $version = $self->_get_old_version();

    my @upgrades;

    # determine the path of this module
    my $path = $INC{'GRNOC/TSDS/Upgrade.pm'};

    # determine the directory where upgrade modules will live in
    $path =~ s/\.pm$//;

    # get a list of all files in that directory
    opendir( my $fh, $path ) or die $!;

    while ( my $file = readdir( $fh ) ) {

        # only take into consideration .pm files
        next if ( $file !~ /\.pm$/ );

        # strip off the .pm extension
        $file =~ s/\.pm$//;

        # replace all understore characters with periods
        $file =~ s/_/\./g;

        # skip this one if its not a later version than the one we're running
        next if ( versioncmp( $version, $file ) >= 0 );

        push( @upgrades, $file );
    }

    close( $fh );

    # sort the upgrades in ascending order
    @upgrades = sort { versioncmp( $a, $b ) } @upgrades;

    return @upgrades;
}

1;
