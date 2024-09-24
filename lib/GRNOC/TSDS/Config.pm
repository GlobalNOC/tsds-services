package GRNOC::TSDS::Config;

use Moo;
use Types::Standard qw(Str Object);

use GRNOC::Config;
use GRNOC::Log;


has config => (
    is  => 'rw',
    isa => Object
);

has config_file => (
    is  => 'rw',
    isa => Str
);


=head2 BUILD

GRNOC::TSDS::Config acts as abstraction to support both ENV and
configuration file based configurations.

The mongo/readwrite user is used for all mongodb_* config file based
results.

=cut
sub BUILD {
    my ($self, $args) = @_;

    if (defined $args->{config_file}) {
	$self->config(new GRNOC::Config(
           config_file => $args->{config_file},
           force_array => 0
	));
    }

    return $self;
}


sub mongodb_uri {
    return $ENV{MONGODB_URI};
}


sub mongodb_user {
    my $self = shift;

    if (exists $ENV{MONGODB_USER}) {
	return $ENV{MONGODB_USER};
    } else {
	return $self->config->get('/config/mongo/readwrite/@user');
    }
}


sub mongodb_pass {
    my $self = shift;

    if (exists $ENV{MONGODB_PASS}) {
	return $ENV{MONGODB_PASS};
    } else {
	return $self->config->get('/config/mongo/readwrite/@password');
    }
}


sub mongodb_root_user {
    my $self = shift;

    if (exists $ENV{MONGODB_USER}) {
	return $ENV{MONGODB_USER};
    } else {
	return $self->config->get('/config/mongo/root/@user');
    }
}


sub mongodb_root_pass {
    my $self = shift;

    if (exists $ENV{MONGODB_PASS}) {
	return $ENV{MONGODB_PASS};
    } else {
	return $self->config->get('/config/mongo/root/@password');
    }
}


sub mongodb_host {
    my $self = shift;

    if (exists $ENV{MONGODB_HOST}) {
	return $ENV{MONGODB_HOST};
    } else {
	return $self->config->get('/config/mongo/@host');
    }
}


sub mongodb_port {
    my $self = shift;

    if (exists $ENV{MONGODB_PORT}) {
	return $ENV{MONGODB_PORT};
    } else {
	return $self->config->get('/config/mongo/@port');
    }
}


sub rabbitmq_host {
    my $self = shift;

    if (exists $ENV{RABBITMQ_HOST}) {
	return $ENV{RABBITMQ_HOST};
    } else {
	return $self->config->get('/config/rabbit/@host');
    }
}


sub rabbitmq_port {
    my $self = shift;

    if (exists $ENV{RABBITMQ_PORT}) {
	return $ENV{RABBITMQ_PORT};
    } else {
	return $self->config->get('/config/rabbit/@port');
    }
}


sub redis_host {
    my $self = shift;

    if (exists $ENV{REDIS_HOST}) {
	return $ENV{REDIS_HOST};
    } else {
	return $self->config->get('/config/redis/@host');
    }
}


sub redis_port {
    my $self = shift;

    if (exists $ENV{REDIS_PORT}) {
	return $ENV{REDIS_PORT};
    } else {
	return $self->config->get('/config/redis/@port');
    }
}

sub memcached_host {
    my $self = shift;

    if (exists $ENV{MEMCACHED_HOST}) {
	return $ENV{MEMCACHED_HOST};
    } else {
	return $self->config->get('/config/memcache/@host');
    }
}


sub memcached_port {
    my $self = shift;

    if (exists $ENV{MEMCACHED_PORT}) {
	return $ENV{MEMCACHED_PORT};
    } else {
	return $self->config->get('/config/memcache/@port');
    }
}

1;
