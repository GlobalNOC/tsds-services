package GRNOC::TSDS::Setup;

use Moo;

use GRNOC::TSDS;
use GRNOC::CLI;
use GRNOC::Config;

use GRNOC::TSDS::Install;

use Data::Dumper;
use MongoDB;
use Sort::Versions;
use Sys::Hostname;
### required attributes ###

has config_file => ( is => 'ro',
    required => 1 );

has unattended => ( is => 'ro',
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

}

### public methods ###

sub upgrade{

    my ( $self ) = @_;

    $self->cli->clear_screen();
    $self->_print_banner();

    # -----
    my $sleep_time = 5;
    # -----


    print "\n Creating mongodb-org-3.6.repo file in /etc/yum.repos.d directory";
    if(system("(
            echo [mongodb-org-3.6]
            echo name=MongoDB Repository
            echo baseurl=https://repo.mongodb.org/yum/redhat/7/mongodb-org/3.6/x86_64/
            echo gpgcheck=1
            echo enabled=1
            echo gpgkey=https://www.mongodb.org/static/pgp/server-3.6.asc
            )> /etc/yum.repos.d/mongodb-org-3.6.repo")!=0)
    {
        print "\n ERROR: Could not write to /etc/yum.repos.d/mongodb-org-3.6.repo";
        exit;
    }

    if(system("yum install -y mongodb-org")!=0){
        print "\n ERROR: Setup failed while installing mongodb-org";
        exit;
    }

    print "\n Changing ownership of all files in /var/lib/mongo/ from root to mongod";
    if(system("chown mongod:mongod /var/lib/mongo/*")!=0){
        print "\n Error occured while changing ownership of all the files in /var/lib/mongo/ from root to mongod";
        exit;
    }


    my $is_correct = "n";
    my $hostname;
    # my $ip;
    my $ip= "127.0.0.1";
    while ($is_correct eq "n") {


        print "\n Please enter the accurate information below";
        print "\n Execute 'hostname' command to get the hostname";
        print "\n What is your hostname?\n ";
        $hostname = <>;
        chomp $hostname;

        print "\n Please enter the ip address of the hostname entered above";
        print "\n What is your hostname?\n";
        # $ip = <>;
        # chomp $ip;

        print "\n Is the hostname and the IP address entered above are correct? \n";
        print "\n Please enter 'y' for yes and 'n' for no.\n ";
        $is_correct = <>;
        chomp $is_correct;
    }

    print "\n Installing gnutls package for certtool to work";
    if(system("yum -y install gnutls-utils.x86_64 rsyslog-gnutls ")!=0){
        print "\n Error occured while installing yum -y install gnutls-utils.x86_64 rsyslog-gnutls";
        exit;
    }
    print "\n Creating key using certtool";
    if(system("certtool -p --outfile /etc/pki/tls/private/mongo-$hostname.key")!=0){
        print "\n Could not create a key file";
        exit;
    }

    print "\n For the next step ignore all the questions except for common name";
    print "\n Make sure to specify the proper hostname for the Common name option";

    if(system("certtool -s --load-privkey /etc/pki/tls/private/mongo-$hostname.key --outfile /etc/pki/tls/certs/mongo-$hostname.crt")!=0){
        print "\n Could not create a certificate file from key file";
        exit;
    }

    print "\n Appending cert file to key file";
    if(system("cat /etc/pki/tls/certs/mongo-$hostname.crt >> /etc/pki/tls/private/mongo-$hostname.key")!=0){
        print "\n Error occured while concatenating certificate file and key file";
        exit;
    }

    print "\n Changing ownership of cert file to mongod";
    if(system("chown mongod:mongod /etc/pki/tls/certs/mongo-$hostname.crt")!=0 || system("chown mongod:mongod /etc/pki/tls/private/mongo-$hostname.key")!=0){
        print "\n Error occured while changing ownership of cert and key files to mongod";
        exit;
    }

    print "\n MongoDB Authorization Configuration";
    print "\n Generating Key file to secure authorization between mongo instances";

    if(system("openssl rand -base64 741 > /etc/mongodb-keyfile")!=0){
        print "\n Error occured while creating keyfile";
        exit;
    }

    print "\n Changing permissions of key file";

    if(system("chown mongod:mongod  /etc/mongodb-keyfile")!=0){
        print "\n Error occured while changing permissions of keyfile";
        exit;
    }

    if(system("chmod 600 /etc/mongodb-keyfile")!=0){
        print "\n Error occured while changing permissions of keyfile";
        exit;
    }
    if(system("systemctl daemon-reload")!=0){
        print "\n Error occured while reloading daemons";
        exit;
    }

    print "\n Enter the number of config server and shard to setup (upto 3 supported by default):";
    my $no_of_shard = <>;
    chomp $no_of_shard;

    print "\n No of shards: $no_of_shard";
    if ($no_of_shard == 0 || $no_of_shard > 3) {
        print "\n Incorrect number of shards: $no_of_shard. Set to 1 by default";
        $no_of_shard = 1;

    }

    print "\n MongoDB Config Server Configuration";
    print "\n Making necessary changes to mongo config files /etc/init.d directory";
    my $server_port;

    my $seconds;
    for (my $i=1; $i <= $no_of_shard; $i++) {
        my $old_str = "/path/to/CA.crt";
        my $new_str = "/etc/pki/tls/certs/mongo-$hostname.crt";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-config$i.conf file";
            exit;
        }

        $old_str = "/path/to/file.pem";
        $new_str = "/etc/pki/tls/private/mongo-$hostname.key";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-config$i.conf file";
            exit;
        }

        $old_str =  "clusterFile:";
        $new_str =  "allowInvalidCertificates: \"true\"";
        my $p = 4;
        my $suffix =  ( ' ' x $p ) . $new_str;

        if(system("sed -i '/$old_str/a $suffix' /etc/mongod-config$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-config$i.conf file";
            exit;
        }

        $old_str =  "allowInvalidCertificates: \"true\"";
        $new_str = "$suffix";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-config$i.conf file";
            exit;
        }

        $old_str = "clusterPassword: \"<password on CA file>\"";
        $new_str = "";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-config$i.conf file";
            exit;
        }

        $old_str = "/path/to/mongodb/keyfile";
        $new_str = "/etc/mongodb-keyfile";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-config$i.conf file";
            exit;
        }

        # print "\n Removing sharding and cluster role from mongod-config$i file";
        # $old_str = "sharding:";
        # $new_str = "";

        # if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
        #     print "\n Error occured while editing /etc/mongod-config$i.conf file";
        #     exit;
        # }

        # $old_str = " clusterRole: \"configsvr\"";
        # $new_str = "";

        # if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
        #     print "\n Error occured while editing /etc/mongod-config$i.conf file";
        #     exit;
        # }

        # print "\n Removing sharding and cluster role from mongod-config$i file";
        # $old_str = "replication:";
        # $new_str = "";

        # if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
        #     print "\n Error occured while editing /etc/mongod-config$i.conf file";
        #     exit;
        # }

        # $old_str = "replSetName: \"cfgsvr\"";
        # $new_str = "";

        # if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
        #     print "\n Error occured while editing /etc/mongod-config$i.conf file";
        #     exit;
        # }

        print "\n Starting mongod-config$i\n";

        if(system("service mongod-config$i restart")!=0){
            print "\n Error occured while starting mongod-config$i";
            exit;
        }

        $seconds = $sleep_time;
        while ($seconds != 0) {
            print "\n Continuing in $seconds..";
            sleep(1);
            $seconds = $seconds - 1;

        }
        # sleep(5);
        # default starts from 27019
        $server_port = 27018 + $i;

        print "\n Initiating the replica set for mongod-config $server_port\n ";
        if(system("mongo --port $server_port --eval \"printjson(rs.initiate());\" ")!=0){
            print "\n Error occured while initializing replica set $server_port";
            exit;
        }

    }
    $seconds = $sleep_time;
    while ($seconds != 0) {
        print "\n Continuing in $seconds..";
        sleep(1);
        $seconds = $seconds - 1;

    }

    print "\n Mondod confing-servers started.. ";
    print "\n MongoDB Mongos Configuration";
    print "\n Making necessary changes to mongos.conf file in /etc/ directory";
    my $old_str = "/path/to/CA.crt";
    my $new_str = "/etc/pki/tls/certs/mongo-$hostname.crt";

    if(system("sed -i 's#$old_str#$new_str#g' /etc/mongos.conf")!=0){
        print "\n Error occured while editing /etc/mongos.conf file";
        exit;
    }

    $old_str = "/path/to/file.pem";
    $new_str = "/etc/pki/tls/private/mongo-$hostname.key";

    if(system("sed -i 's#$old_str#$new_str#g' /etc/mongos.conf")!=0){
        print "\n Error occured while editing /etc/mongos.conf file";
        exit;
    }

    $old_str =  "clusterFile:";
    $new_str =  "allowInvalidCertificates: \"true\"";
    my $p = 4;
    my $suffix =  ( ' ' x $p ) . $new_str;

    if(system("sed -i '/$old_str/a $suffix' /etc/mongos.conf")!=0){
        print "\n Error occured while editing /etc/mongos.conf file";
        exit;
    }

    $old_str =  "allowInvalidCertificates: \"true\"";
    $new_str = "$suffix";

    if(system("sed -i 's#$old_str#$new_str#g' /etc/mongos.conf")!=0){
        print "\n Error occured while editing /etc/mongos.conf file";
        exit;
    }

    $old_str = "clusterPassword: \"<password on CA file>\"";
    $new_str = "";

    if(system("sed -i 's#$old_str#$new_str#g' /etc/mongos.conf")!=0){
        print "\n Error occured while editing /etc/mongos.conf file";
        exit;
    }

    $old_str = "/path/to/mongodb/keyfile";
    $new_str = "/etc/mongodb-keyfile";

    if(system("sed -i 's#$old_str#$new_str#g' /etc/mongos.conf")!=0){
        print "\n Error occured while editing /etc/mongos.conf file";
        exit;
    }

    # $old_str = "localhost";
    # $new_str = $hostname;
    # if(system("sed -i 's#$old_str#$new_str#g' /etc/mongos.conf")!=0){
    #     print "\n Error occured while editing /etc/mongos.conf file";
    #     exit;
    # }

    $old_str = "configDB: \"cfgsvr/localhost:27019,localhost:27020,localhost:27021\"";
    $new_str = "configDB: \"cfgsvr/";
    for (my $i=1; $i <= $no_of_shard; $i++) {

        $server_port = 27018 + $i;

        $new_str = $new_str.$hostname.':'.$server_port;

        if ($i != $no_of_shard) {
            $new_str = $new_str.',';
        }
    }

    $new_str = $new_str.'"';

    if(system("sed -i 's#$old_str#$new_str#g' /etc/mongos.conf")!=0){
        print "\n Error occured while editing configDB in /etc/mongos.conf file";
        exit;
    }

    print "\n Adding fqdn to /etc/hosts for mongos to start";
    my $full_hostname = $hostname;
    my @words = split(/\./,$full_hostname);
    $old_str = "127.0.0.1";
    $new_str = "127.0.0.1 $full_hostname";
    if(system("sed -i 's#$old_str#$new_str#g' /etc/hosts")!=0){
        print "\n Error occured while editing /etc/hosts file";
        exit;
    }

    print "\n Starting mongos \n";

    if(system("service mongos restart")!=0){
        print "\n Error occured while starting mongos";
        exit;
    }
    $seconds = $sleep_time;
    while ($seconds != 0) {
        print "\n Continuing in $seconds..";
        sleep(1);
        $seconds = $seconds - 1;
    }
    print "\n mongos started.. \n";

    # print "\n No admin or root user has been created yet";
    # print "\n Adding root user to MongoDB";

    # print "\n Enter password for the root user";
    # my $root_pwd = $self->cli->get_password("Password");
    # chomp $root_pwd;

    # if(system("mongo admin --eval \"printjson(db.createUser({user: 'root', pwd: '$root_pwd', roles: [{role: 'root', db: 'admin'}]}));\" ")!=0){
    #     print "\n Error occured while adding root user to MongoDB";
    #     exit;
    # }

    # print "\n Editing credentials for root user in /etc/grnoc/tsds/services/config.xml \n";
    # $old_str = "user=\\\"root\\\" password=\\\"password\\\"";
    # $new_str = "user=\\\"root\\\" password=\\\"$root_pwd\\\"";

    # if(system("sed -i 's!$old_str!$new_str!g' /etc/grnoc/tsds/services/config.xml")!=0){
    #     print "\n Error occured while editing /etc/grnoc/tsds/services/config.xml file";
    #     exit;
    # }

    # print "\n Enter a password for read only user :";
    # my $tsds_ro_pwd = $self->cli->get_password("Password");
    # chomp $tsds_ro_pwd;
    # print "\n Enter a password for read write user :";
    # my $tsds_rw_pwd =  $self->cli->get_password("Password");
    # chomp $tsds_rw_pwd;

    # print "\n Editing credentials for read only user";
    # $old_str = "user=\\\"tsds_ro\\\" password=\\\"password\\\"";
    # $new_str = "user=\\\"tsds_ro\\\" password=\\\"$tsds_ro_pwd\\\"";

    # if(system("sed -i 's!$old_str!$new_str!g' /etc/grnoc/tsds/services/config.xml")!=0){
    #     print "\n Error occured while editing /etc/grnoc/tsds/services/config.xml file";
    #     exit;
    # }

    # print "\n Editing credentials for read write user";
    # $old_str = "user=\\\"tsds_rw\\\" password=\\\"password\\\"";
    # $new_str = "user=\\\"tsds_rw\\\" password=\\\"$tsds_rw_pwd\\\"";

    # if(system("sed -i 's!$old_str!$new_str!g' /etc/grnoc/tsds/services/config.xml")!=0){
    #     print "\n Error occured while editing /etc/grnoc/tsds/services/config.xml file";
    #     exit;
    # }

    print "\n MongoDB Sharding Configuration";
    print "\n Making necessary changes to mongod-shard config files /etc/ directory";
    for (my $i=1; $i <= $no_of_shard; $i++) {
        my $old_str = "/path/to/CA.crt";
        my $new_str = "/etc/pki/tls/certs/mongo-$hostname.crt";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-shard$i.conf file";
            exit;
        }

        $old_str = "/path/to/file.pem";
        $new_str = "/etc/pki/tls/private/mongo-$hostname.key";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-shard$i.conf file";
            exit;
        }

        $old_str =  "clusterFile:";
        $new_str =  "allowInvalidCertificates: \"true\"";
        my $p = 4;
        my $suffix =  ( ' ' x $p ) . $new_str;

        if(system("sed -i '/$old_str/a $suffix' /etc/mongod-shard$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-shard$i.conf file";
            exit;
        }

        $old_str =  "allowInvalidCertificates: \"true\"";
        $new_str = "$suffix";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-shard$i.conf file";
            exit;
        }

        $old_str = "clusterPassword: \"<password on CA file>\"";
        $new_str = "";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-shard$i.conf file";
            exit;
        }

        $old_str = "/path/to/mongodb/keyfile";
        $new_str = "/etc/mongodb-keyfile";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-shard$i.conf file";
            exit;
        }

        # print "\n Removing sharding and cluster role from mongod-shard$i file";
        # $old_str = "sharding:";
        # $new_str = "";

        # if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){
        #     print "\n Error occured while editing /etc/mongod-shard$i.conf file";
        #     exit;
        # }

        # $old_str = "clusterRole: shardsvr";

        # # $new_str = "clusterRole: \"cfgsvr\"";
        # $new_str = "";

        # if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){
        #     print "\n Error occured while editing /etc/mongod-shard$i.conf file";
        #     exit;
        # }

        print "\n Starting mongod-shard$i \n";

        if(system("service mongod-shard$i start")!=0){
            print "\n Error occured while starting mongod-shard$i";
            exit;
        }

        $seconds = $sleep_time;
        while ($seconds != 0) {
            print "\n Continuing in $seconds..";
            sleep(1);
            $seconds = $seconds - 1;
        }

        # Default starts from 27025
        $server_port = 27024 + $i;


        print "\n Adding shard with ip $ip  and port $server_port \n";
        if(system("mongo --eval \"printjson(sh.addShard('$ip:$server_port'));\" ")!=0){
            print "\n Error occured while adding shard with ip $ip and port $server_port to mongo";
            exit;
        }
    }


    print "\n No admin or root user has been created yet";
    print "\n Adding root user to MongoDB";

    print "\n Enter password for the root user";
    my $root_pwd = $self->cli->get_password("Password");
    chomp $root_pwd;

    if(system("mongo admin --eval \"printjson(db.createUser({user: 'root', pwd: '$root_pwd', roles: [{role: 'root', db: 'admin'}]}));\" ")!=0){
        print "\n Error occured while adding root user to MongoDB";
        exit;
    }

    print "\n Editing credentials for root user in /etc/grnoc/tsds/services/config.xml \n";
    $old_str = "user=\\\"root\\\" password=\\\"password\\\"";
    $new_str = "user=\\\"root\\\" password=\\\"$root_pwd\\\"";

    if(system("sed -i 's!$old_str!$new_str!g' /etc/grnoc/tsds/services/config.xml")!=0){
        print "\n Error occured while editing /etc/grnoc/tsds/services/config.xml file";
        exit;
    }

    print "\n Enter a password for read only user :";
    my $tsds_ro_pwd = $self->cli->get_password("Password");
    chomp $tsds_ro_pwd;
    print "\n Enter a password for read write user :";
    my $tsds_rw_pwd =  $self->cli->get_password("Password");
    chomp $tsds_rw_pwd;

    print "\n Editing credentials for read only user";
    $old_str = "user=\\\"tsds_ro\\\" password=\\\"password\\\"";
    $new_str = "user=\\\"tsds_ro\\\" password=\\\"$tsds_ro_pwd\\\"";

    if(system("sed -i 's!$old_str!$new_str!g' /etc/grnoc/tsds/services/config.xml")!=0){
        print "\n Error occured while editing /etc/grnoc/tsds/services/config.xml file";
        exit;
    }

    print "\n Editing credentials for read write user";
    $old_str = "user=\\\"tsds_rw\\\" password=\\\"password\\\"";
    $new_str = "user=\\\"tsds_rw\\\" password=\\\"$tsds_rw_pwd\\\"";

    if(system("sed -i 's!$old_str!$new_str!g' /etc/grnoc/tsds/services/config.xml")!=0){
        print "\n Error occured while editing /etc/grnoc/tsds/services/config.xml file";
        exit;
    }

    print "Performing TSDS Databse Bootstrap";

    if(system("/usr/bin/tsds_install.pl")!=0){
        print "\n Error occured while performing TSDS Databse Bootstrap";
        exit;
    }

    print "\n Memcached Installation";
    print "\n Installing memcached";

    if(system("yum install -y memcached")!=0){
        print "Error occured while installing memcached server";
        exit;
    }

    print "\n Turning on memcached server";

    if(system("service memcached start")!=0){
        print "Error occured while starting memcached server";
        exit;
    }

    print "\n Redis Installation";
    print "\n Installing redis";

    if(system("yum install -y redis")!=0){
        print "Error occured while installing redis";
        exit;
    }

    print "\n Turning on redis";

    if(system("service redis start")!=0){
        print "Error occured while starting redis";
        exit;
    }

    print "\n RabbitMQ Installation";
    print "\n Installing rabbitmq server";

    if(system("yum install -y rabbitmq-server")!=0){
        print "Error occured while installing rabbitmq-server";
        exit;
    }

    print "\n Enabling rabbitmq-management plugin";

    if(system("touch /etc/rabbitmq/enabled_plugins")!=0){
        print "Error occured while creating /etc/rabbitmq/enabled_plugins file";
        exit;
    }

    if(system("echo [rabbitmq_management]. >> /etc/rabbitmq/enabled_plugins")!=0){
        print "Error occured while editing /etc/rabbitmq/enabled_plugins file";
        exit;
    }
    my $filename = '/etc/rabbitmq/rabbitmq.config';
    if(-e $filename) {
        print "\n Rabbitmq config file exists";
    }
    else
    {
        print "\n Creating rabbitmq config file";
        if(system("touch /etc/rabbitmq/rabbitmq.config")!=0){
            print "Error occured while creating /etc/rabbitmq/rabbitmq.config file";
            exit;
        }
        if(system("(
                echo [
                echo {mnesia, [{dump_log_write_threshold, 1000}]},
                echo {rabbit, [
                echo      {tcp_listeners, [5672]},
                echo {hipe_compile, false},
                echo {vm_memory_high_watermark, 0.75},
                echo {vm_memory_high_watermark_paging_ratio, 0.75},
                echo {disk_free_limit, \\\"500MB\\\"}
                echo ]}
                echo ].
                )>> /etc/rabbitmq/rabbitmq.config")!=0)
        {
            print "\n Error occured while editing /etc/rabbitmq/rabbitmq.config file";
            exit;
        }
    }
    print "\n Adding hostname to /etc/hosts ";

    $old_str = "127.0.0.1";
    $new_str = "127.0.0.1  $words[0]";
    if(system("sed -i 's#$old_str#$new_str#g' /etc/hosts")!=0){
        print "\n Error occured while editing /etc/hosts file";
        exit;
    }

    print "\n Starting RabbitMQ server";
    if(system("service rabbitmq-server start")!=0)
    {
        print "\n Error occured while starting rabbitmq-server";
        exit;
    }

    print "\n Starting TSDS Writer Service";
    if(system("service tsds_writer start")!=0)
    {
        print "\n Error occured while starting tsds_writer";
        exit;
    }

    print "\n Editing /etc/httpd/conf/httpd.conf file";
    if(system("(
            echo \\<VirtualHost *:80\\>
            )>> /etc/httpd/conf/httpd.conf ")!=0)
    {
        print("\n Error occured while editing /etc/httpd/conf/httpd.conf file");
        exit;
    }

    print "\n checking if files exist before adding them to httpd.conf";
    $filename = '/etc/httpd/conf.d/grnoc/yui.conf';
    if(-e $filename) {
        print "\n /etc/httpd/conf.d/grnoc/yui.conf file exists!";
        if(system("(
                echo INCLUDE conf.d/grnoc/yui.conf
                )>> /etc/httpd/conf/httpd.conf ")!=0)
        {
            print("\n Error occured while editing /etc/httpd/conf/httpd.conf file");
            exit;
        }
    }

    $filename = '/etc/httpd/conf.d/grnoc/glue.conf';
    if(-e $filename) {
        print "\n /etc/httpd/conf.d/grnoc/glue.conf file exists!";
        if(system("(
                echo INCLUDE conf.d/grnoc/glue.conf
                )>> /etc/httpd/conf/httpd.conf ")!=0)
        {
            print("\n Error occured while editing /etc/httpd/conf/httpd.conf file");
            exit;
        }
    }
    if(system("(
            echo INCLUDE conf.d/grnoc/tsds-services-temp.conf
            echo \\</VirtualHost\\>
            )>> /etc/httpd/conf/httpd.conf ")!=0)
    {
        print("\n Error occured while editing /etc/httpd/conf/httpd.conf file");
        exit;
    }

    print "\n Starting httpd service";
    if(system("service httpd start")!=0){
        print "\n Error occured while starting httpd";
        exit;
    }

    print "\n Installing Sphinx";
    if(system("yum -y install sphinx")!=0)
    {
        print "\n Error occured while installing Sphinx";
        exit;
    }

    print "\n Configuring Sphinx search";
    if(system("cp /etc/sphinx/sphinx.conf.tsds /etc/sphinx/sphinx.conf")!=0){
        print "\n Error occured while configuring sphinx search \n cp /etc/sphinx/sphinx.conf.tsds /etc/sphinx/sphinx.conf ";
        exit;
    }

    if(system("/usr/bin/indexer tsds_metadata_index")!=0){
        print "\n Error occured while running configuring spinx search \n  /usr/bin/indexer tsds_metadata_index";
        exit;
    }

    if(system("/usr/bin/indexer tsds_metadata_delta_index")!=0){
        print "\n Error occured while configuring sphinx search \n /usr/bin/indexer tsds_metadata_delta_index";
        exit;
    }

    print "\n Starting searchd ";

    if(system("service searchd start")!=0){
        print "\n Error occured while starting searchd";
        exit;
    }

    my $old_cron = "# */5 * * * * root /usr/bin/indexer tsds_metadata_delta_index --rotate > /dev/null 2>&1";
    my $new_cron = "*/5 * * * * root /usr/bin/indexer tsds_metadata_delta_index --rotate > /dev/null 2>&1";
    if(system("sed -i 's!$old_cron!$new_cron!g' /etc/cron.d/tsds-services.cron")!=0){
        print "\n Error occured while editing /etc/cron.d/tsds-services.cron file";
        exit;
    }

    $old_cron = "# 1,31 * * * * root /usr/bin/indexer --merge tsds_metadata_index tsds_metadata_delta_index --rotate > /dev/null 2>&1";
    $new_cron = "1,31 * * * * root /usr/bin/indexer --merge tsds_metadata_index tsds_metadata_delta_index --rotate > /dev/null 2>&1";
    if(system("sed -i 's!$old_cron!$new_cron!g' /etc/cron.d/tsds-services.cron")!=0){
        print "\n Error occured while editing /etc/cron.d/tsds-services.cron file";
        exit;
    }

    print "\n Installing TSDS Aggregate Configuration";
    if(system("yum -y install grnoc-tsds-aggregate")!=0){
        print "\n Error occured while installing grnoc-tsds-aggregate";
        exit;
    }

    print "\n Changing mongo configuration to point to current user and password configured in /etc/grnoc/tsds/services/config.xml";
    my $old_user = "needs_rw";
    my $new_user = "root";
    my $old_pwd = "rw_password";
    if(system("sed -i 's!$old_user!$new_user!g' /etc/grnoc/tsds/aggregate/config.xml")!=0){
        print "\n Error occured while editing /etc/grnoc/tsds/aggregate/config.xml file";
        exit;
    }
    if(system("sed -i 's!$old_pwd!$root_pwd!g' /etc/grnoc/tsds/aggregate/config.xml")!=0){
        print "\n Error occured while editing /etc/grnoc/tsds/aggregate/config.xml file";
        exit;
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
    $self->_print_title( "Global Research NOC TSDS Setup - Copyright(C) $year The Trustees of Indiana University" );
}

sub _print_title {

    my ( $self, $title ) = @_;
    my $len = length( $title );

    print "$title\n";

    # print some dash characters underneath the title
    print '=' x $len, "\n\n";
}


1;
