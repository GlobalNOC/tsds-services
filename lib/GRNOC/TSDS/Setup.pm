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

sub setup {

    my ( $self ) = @_;

    $self->cli->clear_screen();
    $self->_print_banner();

    print "\n Creating mongodb-org-3.0.repo file in /etc/yum.repos.d directory";
    if(system("(                                                                                                                                             
                echo [mongodb-org-3.0]                                                                                                                                   
                echo name=MongoDB 3.0 Repository                                                                                                                         
                echo baseurl=https://repo.mongodb.org/yum/redhat/6/mongodb-org/3.0/x86_64/                                                                               
                echo gpgcheck=0                                                                                                                                          
                echo enabled=1                                                                                                                                           
            )>> /etc/yum.repos.d/mongodb-org-3.0.repo")!=0)
    {
	print "\n ERROR: Could not write to /etc/yum.repos.d/mongodb-org-3.0.repo";
    }

    if(system("yum install -y mongodb-org")!=0){
        print "\n ERROR: Setup failed while installing mongodb-org";
    }

    print "\n Changing ownership of all files in /var/lib/mongod/ from root to mongod";
    if(system("chown mongod:mongod /var/lib/mongo/*")!=0)
        print "\n Error occured while changing ownership of all the files in /var/lib/mongo/ from root to mongod";
    }

    print "what is your hostname?";
    my $hostname = <>;
    chomp $hostname;
    
    print "\n Installing gnutls package for certtool to work";
    if(system("yum -y install gnutls-utils.x86_64 rsyslog-gnutls ")!=0){
	print "\n Error occured while installing yum -y install gnutls-utils.x86_64 rsyslog-gnutls";
    }
    print "\n Creating key using certtool";
    if(system("certtool -p --outfile /etc/pki/tls/private/mongo-$hostname.key")!=0){
        print "\n Could not create a key file";
    }

    print "\n For the next step ignore all the questions except for common name";
    print "\n Make sure to specify the proper hostname for the Common name option";

    if(system("certtool -s --load-privkey /etc/pki/tls/private/mongo-$hostname.key --outfile /etc/pki/tls/certs/mongo-$hostname.crt")!=0){
        print "\n Could not create a certificate file from key file";
    }
    
    print "\n Appending cert file to key file"; 
    if(system("cat /etc/pki/tls/certs/mongo-$hostname.crt >> /etc/pki/tls/private/mongo-$hostname.key")!=0){
	print "\n Error occured while concatenating certificate file and key file";
    }
    
    print "\n Changing ownership of cert file to mongod";
    if(system("chown mongod:mongod /etc/pki/tls/certs/mongo-$hostname.crt")!=0 || system("chown mongod:mongod /etc/pki/tls/private/mongo-$hostname.key")!=0){
        print "\n Error occured while changing ownership of cert and key files to mongod";
    }
    
    print "\n MongoDB Authorization Configuration";
    print "\n Generating Key file to secure authorization between mongo instances";

    if(system("openssl rand -base64 741 > /etc/mongodb-keyfile")!=0){
        print "\n Error occured while creating keyfile";
    }

    print "\n Changing permissions of key file";

    if(system("chown mongod:mongod  /etc/mongodb-keyfile")!=0){
        print "\n Error occured while changing permissions of keyfile";
    }

    if(system("chmod 600 /etc/mongodb-keyfile")!=0){                                                                                                                     
        print "\n Error occured while changing permissions of keyfile";                                                                                                  
    }  
    print "\n MongoDB Config Server Configuration";
    print "\n Making necessary changes to mongo config files /etc/init.d directory";

    for (my $i=1; $i <= 3; $i++) {
	my $old_str = "/path/to/CA.crt";
	my $new_str = "/etc/pki/tls/certs/mongo-$hostname.crt";

	if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){                                                    
	    print "\n Error occured while editing /etc/mongd-config$i.conf file";
	} 

	$old_str = "/path/to/file.pem";
	$new_str = "/etc/pki/tls/private/mongo-$hostname.key";

	if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
	    print "\n Error occured while editing /etc/mongod-config$i.conf file";
	}

	$old_str =  "clusterFile:";
	$new_str =  "allowInvalidCertificates: \"true\"";
	my $p = 4;
	my $suffix =  ( ' ' x $p ) . $new_str;

	if(system("sed -i '/$old_str/a $suffix' /etc/mongod-config$i.conf")!=0){
	    print "\n Error occured while editing /etc/mongod-config$i.conf file";
	}

	$old_str =  "allowInvalidCertificates: \"true\"";
	$new_str = "$suffix";

	if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
	    print "\n Error occured while editing /etc/mongod-config$i.conf file";
	}
    
	$old_str = "clusterPassword: \"<password on CA file>\"";
	$new_str = "";

	if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
	    print "\n Error occured while editing /etc/mongod-config$i.conf file";
	}

	$old_str = "/path/to/mongodb/keyfile";
	$new_str = "/etc/mongodb-keyfile";

	if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
	    print "\n Error occured while editing /etc/mongod-config$i.conf file";
	}
        
	print "\n Removing sharding and cluster role from mongod-config$i file";
	$old_str = "sharding:";
        $new_str = "";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-config$i.conf file";
        }

	$old_str = " clusterRole: \"configsvr\"";
        $new_str = "";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-config$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-config$i.conf file";
        }

        print "\n Starting mongod-config$i";

        if(system("service mongod-config$i start")!=0){
	    print "\n Error occured while starting mongod-config$i";
   	}
    }

    print "\n MongoDB Mongos Configuration";
    print "\n Making necessary changes to mongos.conf file in /etc/ directory";                                                                                   
    my $old_str = "/path/to/CA.crt";                                                                                                                             
    my $new_str = "/etc/pki/tls/certs/mongo-$hostname.crt";                                                                                                    

    if(system("sed -i 's#$old_str#$new_str#g' /etc/mongos.conf")!=0){                                                                                              
        print "\n Error occured while editing /etc/mongos.conf file";                                                                                             
    }                                                                                                                                                             
    
    $old_str = "/path/to/file.pem";                                                                                                                               
    $new_str = "/etc/pki/tls/private/mongo-$hostname.key";                                                                                                        

    if(system("sed -i 's#$old_str#$new_str#g' /etc/mongos.conf")!=0){                                                                                             
        print "\n Error occured while editing /etc/mongos.conf file";                                                                                         
    }                                                                                                                                                           
    
    $old_str =  "clusterFile:";                                                                                                                                   
    $new_str =  "allowInvalidCertificates: \"true\"";                                                                                                             
    my $p = 4;                                                                                                                                                    
    my $suffix =  ( ' ' x $p ) . $new_str;                                                                                                                        

    if(system("sed -i '/$old_str/a $suffix' /etc/mongos.conf")!=0){                                                                                                
        print "\n Error occured while editing /etc/mongos.conf file";                                                                                             
    }                                                                                                                                                             
    
    $old_str =  "allowInvalidCertificates: \"true\"";                                                                                                             
    $new_str = "$suffix";                                                                                                                                         
    
    if(system("sed -i 's#$old_str#$new_str#g' /etc/mongos.conf")!=0){                                                                                            
       print "\n Error occured while editing /etc/mongos.conf file";                                                                                            
    }                                                                                                                                                        

    $old_str = "clusterPassword: \"<password on CA file>\"";                                                                                                      
    $new_str = "";                                                                                                                                                

    if(system("sed -i 's#$old_str#$new_str#g' /etc/mongos.conf")!=0){                                                                                              
        print "\n Error occured while editing /etc/mongos.conf file";                                                                                           
    }                                                                                                                                               
    
    $old_str = "/path/to/mongodb/keyfile";
    $new_str = "/etc/mongodb-keyfile";

    if(system("sed -i 's#$old_str#$new_str#g' /etc/mongos.conf")!=0){
	print "\n Error occured while editing /etc/mongos.conf file";
    }
    
    $old_str = "localhost";
    $new_str = $hostname;
    if(system("sed -i 's#$old_str#$new_str#g' /etc/mongos.conf")!=0){
        print "\n Error occured while editing /etc/mongos.conf file";
    }

    print "\n Adding fqdn to /etc/hosts for mongos to start";
    my $full_hostname = hostname;
    my @words = split(/\./,$full_hostname);
    $old_str = "127.0.0.1";
    $new_str = "127.0.0.1 $full_hostname";
    
    print "\n Starting mongos";

    if(system("service mongos start")!=0){
	print "\n Error occured while starting mongos";
    }
    
    print "\n MongoDB Sharding Configuration";
    print "\n Making necessary changes to mongod-shard config files /etc/ directory";                                                                    
    for (my $i=1; $i <= 3; $i++) {                                                                                                                             
        my $old_str = "/path/to/CA.crt";                                                                                                                         
        my $new_str = "/etc/pki/tls/certs/mongo-$hostname.crt";                                                                                                  

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){                                                                                
                 print "\n Error occured while editing /etc/mongod-shard$i.conf file";                                                                            
        }                                                                                                                                                        

        $old_str = "/path/to/file.pem";                                                                                                                          
        $new_str = "/etc/pki/tls/private/mongo-$hostname.key";                                                                                                   

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){                                                                                 
            print "\n Error occured while editing /etc/mongod-shard$i.conf file";                                                                                
        }                                                                                                                                                        
                                                                                                                                                                 
        $old_str =  "clusterFile:";                                                                                                                              
        $new_str =  "allowInvalidCertificates: \"true\"";                                                                                                        
        my $p = 4;                                                                                                                                               
        my $suffix =  ( ' ' x $p ) . $new_str;                                                                                                                   

        if(system("sed -i '/$old_str/a $suffix' /etc/mongod-shard$i.conf")!=0){                                                                                    
            print "\n Error occured while editing /etc/mongod-shard$i.conf file";                                                                                
        }                                                                                                                                                        

        $old_str =  "allowInvalidCertificates: \"true\"";                                                                                                        
        $new_str = "$suffix";                                                                                                                                    

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){                                                                                  
            print "\n Error occured while editing /etc/mongod-shard$i.conf file";                                                                                
        }                                                                                                                                                        
                                                                                                                                                                 
        $old_str = "clusterPassword: \"<password on CA file>\"";                                                                                                 
        $new_str = "";                                                                                                                                           

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){                                                                                 
            print "\n Error occured while editing /etc/mongod-shard$i.conf file";                                                                
        }

        $old_str = "/path/to/mongodb/keyfile";
        $new_str = "/etc/mongodb-keyfile";                                                                                                    

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){                                                                                   
            print "\n Error occured while editing /etc/mongod-shard$i.conf file";                                                                                 
        }  
	
	print "\n Removing sharding and cluster role from mongod-shard$i file";
        $old_str = "sharding:";
        $new_str = "";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-shard$i.conf file";
        }

        $old_str = " clusterRole: \"configsvr\"";
        $new_str = "";

        if(system("sed -i 's#$old_str#$new_str#g' /etc/mongod-shard$i.conf")!=0){
            print "\n Error occured while editing /etc/mongod-shard$i.conf file";
        }

        print "\n Starting mongod-shard$i";                                                                                                                     

        if(system("service mongod-shard$i start")!=0){                                                                                                            
            print "\n Error occured while starting mongod-shard$i";    
	}
    }                                    

    print "\n Adding shards to mongo";
    print "\n Adding shard with hostname $hostname and port 27025";

    if(system("mongo --eval \"printjson(sh.addShard('$hostname:27025'));\" ")!=0){                                                
	print "\n Error occured while adding shard with port 27025 to mongo";                                                                              
    }

    print "\n Adding shard with hostname $hostname and port 27026";

    if(system("mongo --eval \"printjson(sh.addShard('$hostname:27026'));\" ")!=0){
        print "\n Error occured while adding shard with port 27026 to mongo";
    }

    print "\n Adding shard with hostname $hostname and port 27027";

    if(system("mongo --eval \"printjson(sh.addShard('$hostname:27027'));\" ")!=0){
        print "\n Error occured while adding shard with port 27027 to mongo";
    }

    print "\n No admin or root user has been created yet";
    print "\n Adding root user to MongoDB";

    print "\n Enter password for the root user";
    my $root_pwd = $cli->get_password("Password");
    chomp $root_pwd;

    if(system("mongo admin --eval \"printjson(db.createUser({user: 'root', pwd: '$root_pwd', roles: [{role: 'root', db: 'admin'}]}));\" ")!=0){
        print "\n Error occured while adding root user to MongoDB";
    }

    print "\n Editing credentials for root user in /etc/grnoc/tsds/services/config.xml \n";
    $old_str = "user=\\\"root\\\" password=\\\"password\\\"";
    $new_str = "user=\\\"root\\\" password=\\\"$root_pwd\\\"";

    if(system("sed -i 's!$old_str!$new_str!g' /etc/grnoc/tsds/services/config.xml")!=0){
        print "\n Error occured while editing /etc/grnoc/tsds/services/config.xml file";
    }

    print "\n Enter a password for read only user :";
    my $tsds_ro_pwd = $cli->get_password("Password");
    chomp $tsds_ro_pwd;
    print "\n Enter a password for read write user :";
    my $tsds_rw_pwd =  $cli->get_password("Password");
    chomp $tsds_rw_pwd;

    print "\n Editing credentials for read only user";
    $old_str = "user=\\\"tsds_ro\\\" password=\\\"password\\\"";
    $new_str = "user=\\\"tsds_ro\\\" password=\\\"$tsds_ro_pwd\\\"";

    if(system("sed -i 's!$old_str!$new_str!g' /etc/grnoc/tsds/services/config.xml")!=0){
        print "\n Error occured while editing /etc/grnoc/tsds/services/config.xml file";
    }

    print "\n Editing credentials for read write user";
    $old_str = "user=\\\"tsds_rw\\\" password=\\\"password\\\"";
    $new_str = "user=\\\"tsds_rw\\\" password=\\\"$tsds_rw_pwd\\\"";

    if(system("sed -i 's!$old_str!$new_str!g' /etc/grnoc/tsds/services/config.xml")!=0){
        print "\n Error occured while editing /etc/grnoc/tsds/services/config.xml file";
    }
    
    print "Performing TSDS Databse Bootstrap";

    if(system("/usr/bin/tsds_install.pl")!=0){
        print "\n Error occured while performing TSDS Databse Bootstrap";
    }

    print "\n Memcached Installation";
    print "\n Installing memcached";

    if(system("yum install -y memcached")!=0){
	print "Error occured while installing memcached server";
    }
    
    print "\n Turning on memcached server";

    if(system("service memcached start")!=0){
	print "Error occured while starting memcached server";
    }

    print "\n Redis Installation";
    print "\n Installing redis";

    if(system("yum install -y redis")!=0){
        print "Error occured while installing redis";
    }

    print "\n Turning on redis";

    if(system("service redis start")!=0){
        print "Error occured while starting redis";
    }

    print "\n RabbitMQ Installation";
    print "\n Installing rabbitmq server";

    if(system("yum install -y rabbitmq-server")!=0){
        print "Error occured while installing rabbitmq-server";
    }

    print "\n Enabling rabbitmq-management plugin";
  
    if(system("touch /etc/rabbitmq/enabled_plugins")!=0){
	print "Error occured while creating /etc/rabbitmq/enabled_plugins file";
    }

    if(system("echo [rabbitmq_management]. >> /etc/rabbitmq/enabled_plugins")!=0){
        print "Error occured while editing /etc/rabbitmq/enabled_plugins file";
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
	}
    }
    print "\n Adding hostname to /etc/hosts ";

    $old_str = "127.0.0.1";                                                                                              
    $new_str = "127.0.0.1  $words[0]";                                                                                  
    if(system("sed -i 's#$old_str#$new_str#g' /etc/hosts")!=0){                                                            
  	print "\n Error occured while editing /etc/hosts file";                                                           
    }

    print "\n Starting RabbitMQ server"; 
    if(system("service rabbitmq-server start")!=0)
    {
	print "\n Error occured while starting rabbitmq-server";
    }
   
    print "\n Starting TSDS Writer Service";
    if(system("service tsds_writer start")!=0)
    {   
        print "\n Error occured while starting tsds_writer";
    }

    print "\n Editing /etc/httpd/conf/httpd.conf file";
    if(system("(                                                                                                                                                                      
            echo \\<VirtualHost *:80\\>                                                                                                                                                 
           )>> /etc/httpd/conf/httpd.conf ")!=0)
    {
	print("\n Error occured while editing /etc/httpd/conf/httpd.conf file");
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
	}
    }
    if(system("(                                                                                                                                                                            
             echo INCLUDE conf.d/grnoc/tsds-services-temp.conf                                                                                                                          
             echo \\</VirtualHost\\>                                                                                                                                                    
           )>> /etc/httpd/conf/httpd.conf ")!=0)
    {
	print("\n Error occured while editing /etc/httpd/conf/httpd.conf file");
    }

    print "\n Starting httpd service";
    if(system("service httpd start")!=0){
	print "\n Error occured while starting httpd";
    }

    print "\n Installing Sphinx";
    if(system("yum -y install sphinx")!=0) 
    {
	print "\n Error occured while installing Sphinx";
    }

    print "\n Configuring Sphinx search";
    if(system("cp /etc/sphinx/sphinx.conf.tsds /etc/sphinx/sphinx.conf")!=0){
	print "\n Error occured while configuring sphinx search \n cp /etc/sphinx/sphinx.conf.tsds /etc/sphinx/sphinx.conf ";
    }
    
    if(system("/usr/bin/indexer tsds_metadata_index")!=0){
	print "\n Error occured while running configuring spinx search \n  /usr/bin/indexer tsds_metadata_index";
    }

    if(system("/usr/bin/indexer tsds_metadata_delta_index")!=0){
	print "\n Error occured while configuring sphinx search \n /usr/bin/indexer tsds_metadata_delta_index";
    }
    
    print "\n Starting searchd ";
    
    if(system("service searchd start")!=0){
	print "\n Error occured while starting searchd";
    }

    my $old_cron = "# */5 * * * * root /usr/bin/indexer tsds_metadata_delta_index --rotate > /dev/null 2>&1";
    my $new_cron = "*/5 * * * * root /usr/bin/indexer tsds_metadata_delta_index --rotate > /dev/null 2>&1";
    if(system("sed -i 's!$old_cron!$new_cron!g' /etc/cron.d/tsds-services.cron")!=0){                                              
        print "\n Error occured while editing /etc/cron.d/tsds-services.cron file";                                                     
    }  

    $old_cron = "# 1,31 * * * * root /usr/bin/indexer --merge tsds_metadata_index tsds_metadata_delta_index --rotate > /dev/null 2>&1";
    $new_cron = "1,31 * * * * root /usr/bin/indexer --merge tsds_metadata_index tsds_metadata_delta_index --rotate > /dev/null 2>&1";
    if(system("sed -i 's!$old_cron!$new_cron!g' /etc/cron.d/tsds-services.cron")!=0){                                                  
        print "\n Error occured while editing /etc/cron.d/tsds-services.cron file";                                                     
    }

    print "\n Installing TSDS Aggregate Configuration";
    if(system("yum -y install grnoc-tsds-aggregate")!=0){
	print "\n Error occured while installing grnoc-tsds-aggregate";
    }

    print "\n Changing mongo configuration to point to current user and password configured in /etc/grnoc/tsds/services/config.xml";
    my $old_user = "needs_rw";
    my $new_user = "root";
    my $old_pwd = "rw_password";
    if(system("sed -i 's!$old_user!$new_user!g' /etc/grnoc/tsds/aggregate/config.xml")!=0){                                                                                          
        print "\n Error occured while editing /etc/grnoc/tsds/aggregate/config.xml file";                                                                                           
    } 
    if(system("sed -i 's!$old_pwd!$root_pwd!g' /etc/grnoc/tsds/aggregate/config.xml")!=0){
        print "\n Error occured while editing /etc/grnoc/tsds/aggregate/config.xml file";
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
