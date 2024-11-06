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

    if ($args->{config_file}) {
        $self->config(new GRNOC::Config(
            config_file => $args->{config_file},
            force_array => 0
        ));
    } elsif (defined $args->{config}) {
        die "GRNOC::TSDS::Config - 'config' is not a valid argument";
    }

    return $self;
}


sub tsds_max_decom_procs {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{TSDS_MAX_DECOM_PROCS};
    } else {
        return $self->config->get('/config/decom/@max_procs');
    }
}


sub tsds_writer_procs {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{TSDS_WRITER_PROCS};
    } else {
        return $self->config->get('/config/num-processes');
    }
}


sub tsds_aggregate_writer_procs {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{TSDS_AGGREGATE_WRITER_PROCS};
    } else {
        return $self->config->get('/config/num-aggregate-processes');
    }
}


sub tsds_proxy_users {
    my $self = shift;

    if (!defined $self->config) {
        my $users_str = defined $ENV{TSDS_PROXY_USERS} ? $ENV{TSDS_PROXY_USERS} : '';
        my @users = split(',', $users_str);
        return \@users;
    } else {
        $self->config->{'force_array'} = 1;
        my $proxy_users = $self->config->get('/config/proxy-users/username');
        $self->config->{'force_array'} = 0;
        return $proxy_users;
    }
}


sub tsds_writer_pid_file {
    my $self = shift;

    if (defined $self->config) {
        return $self->config->get('/config/pid-file');
    } else {
        return '/tmp/tsds_writer.pid';
    }
}


sub mongodb_uri {
    my $self = shift;
    return $ENV{MONGODB_URI};
}


sub mongodb_user {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{MONGODB_USER};
    } else {
        return $self->config->get('/config/mongo/readwrite/@user');
    }
}


sub mongodb_pass {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{MONGODB_PASS};
    } else {
        return $self->config->get('/config/mongo/readwrite/@password');
    }
}


sub mongodb_root_user {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{MONGODB_USER};
    } else {
        return $self->config->get('/config/mongo/root/@user');
    }
}


sub tsds_push_users {
    my $self = shift;

    my $push_restrictions = {};

    if (!defined $self->config) {
        return $push_restrictions;
    }

    $self->config->{'force_array'} = 1;
    my $push_names = $self->config->get('/config/push-users/user/@name');

    foreach my $user (@$push_names){
        my $databases = $self->config->get("/config/push-users/user[\@name='$user']/database");

        foreach my $database (@$databases){
            my $db_name  = $database->{'name'};
            my $metadata = $database->{'metadata'} || [];

            my $meta_restrictions = {};
            foreach my $metadata (@$metadata){
                $meta_restrictions->{$metadata->{'field'}} = $metadata->{'pattern'};
            }

            $push_restrictions->{$user}{$db_name} = $meta_restrictions;
        }
    }

    $self->config->{'force_array'} = 0;
    return $push_restrictions;
}


sub mongodb_root_pass {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{MONGODB_PASS};
    } else {
        return $self->config->get('/config/mongo/root/@password');
    }
}


sub mongodb_host {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{MONGODB_HOST};
    } else {
        return $self->config->get('/config/mongo/@host');
    }
}


sub mongodb_port {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{MONGODB_PORT};
    } else {
        return $self->config->get('/config/mongo/@port');
    }
}


sub mongodb_ignore_databases {
    my $self = shift;
    return [
        'admin',
        'test',
        'config',
        'tsds_reports',
        'local',
        'tsds_version'
    ];
}


sub rabbitmq_user {
    my $self = shift;
    return $ENV{RABBITMQ_USER};
}


sub rabbitmq_pass {
    my $self = shift;
    return $ENV{RABBITMQ_PASS};
}


sub rabbitmq_host {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{RABBITMQ_HOST};
    } else {
        return $self->config->get('/config/rabbit/@host');
    }
}


sub rabbitmq_port {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{RABBITMQ_PORT};
    } else {
        return $self->config->get('/config/rabbit/@port');
    }
}


sub rabbitmq_queue {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{RABBITMQ_QUEUE};
    } else {
        return $self->config->get('/config/rabbit/@queue');
    }
}


sub rabbitmq_aggregate_queue {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{RABBITMQ_AGGREGATE_QUEUE};
    } else {
        return $self->config->get('/config/rabbit/@aggregate-queue');
    }
}


sub redis_host {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{REDIS_HOST};
    } else {
        return $self->config->get('/config/redis/@host');
    }
}


sub redis_port {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{REDIS_PORT};
    } else {
        return $self->config->get('/config/redis/@port');
    }
}


sub sphinx_host {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{SPHINX_HOST};
    } else {
        return $self->config->get('/config/sphinx/mysql/@host') | '127.0.0.1';
    }
}


sub sphinx_port {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{SPHINX_PORT};
    } else {
        return $self->config->get('/config/sphinx/mysql/@port') | 9306;
    }
}


sub memcached_host {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{MEMCACHED_HOST};
    } else {
        return $self->config->get('/config/memcache/@host');
    }
}


sub memcached_port {
    my $self = shift;

    if (!defined $self->config) {
        return $ENV{MEMCACHED_PORT};
    } else {
        return $self->config->get('/config/memcache/@port');
    }
}

1;
