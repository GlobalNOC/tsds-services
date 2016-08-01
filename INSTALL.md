# TSDS Services & MongoDB Install Guide

This document covers installing MongoDB in a sharded environment, along with setting up TSDS services on top of it. The steps given below should be followed in the order they are listed unless you are sure you know what you are doing.  This document also assumes a RedHat Linux environment, or one of its derivatives.

An important note about firewalls: in a sharded environment, every shard needs access to every other shard, as well as all of the config servers. This means that if you are using a firewall such as iptables, which is typically turned on and filtering traffic by default, then each participating machine will need to open all of its mongo related ports (except mongos) to every
other participating machine in order for MongoDB to work properly.

- [MongoDB Installation](#mongodb-installation)
- [MongoDB Config Server Configuration](#mongodb-config-server-configuration)
- [MongoDB mongos Configuration](#mongodb-mongos-configuration)
- [MongoDB Sharding Configuration](#mongodb-sharding-configuration)
- [MongoDB Authorization Configuration](#mongodb-authorization-configuration)
- [MongoDB SSL Configuration](#mongodb-ssl-configuration)
- [TSDS Database Bootstrap](#tsds-database-bootstrap)
- [Memcached Installation](#memcached-installation)
- [Redis Installation](#redis-installation)
- [RabbitMQ Installation](#rabbitmq-installation)
- [TSDS Writer Configuration](#tsds-writer-configuration)
- [Apache Configuration](#apache-configuration)
- [TSDS Aggregate Configuration](#tsds-aggregate-configuration)
- [Sphinx Search Configuration](#sphinx-search-configuration)

## MongoDB Installation

Adminstering a MongoDB cluster is much more involved than running many other databases.  It is strongly recommend to familiarize yourself with how MongoDB works, in particular about [sharding](https://docs.mongodb.org/manual/core/sharding-introduction/#sharding-introduction) across multiple machines, and also about redundancy via [replica sets](https://docs.mongodb.org/manual/core/replication-introduction/).  This guide will attempt to walk you through the steps necessary to stand up single-shard MongoDB from scratch, but knowledge about MongoDB will likely be required in order to successfully administer a production TSDS installation.

To install the MongoDB RPM packages, first you will need to add the yum repository that MongoDB provides to your system in order for it to do so.  Follow the docs on installing the [MongoDB Yum Repository](http://docs.mongodb.org/manual/tutorial/install-mongodb-on-red-hat-centos-or-fedora-linux/) for RedHat, CentOS, or Fedora Linux.  Afterward, You should be able to do the following:

```
[root@tsds ~]# yum install mongodb-org
```

This will install all mongodb-related components, including the mongod server as well as CLI based tools.

## MongoDB Config Server Configuration

In a sharded environment, MongoDB requires what are called [config servers](http://docs.mongodb.org/manual/core/sharded-cluster-config-servers/) to manage everything. These are just `mongod` instances that contain metadata about the sharding environment and not the actual user data stored in the database.  MongoDB typically recommends running three config servers on separate hosts for redundancy purposes in a production environment.  At least one config server must be available.

Example init scripts and config files should have been installed with the `grnoc-tsds-services` package for the MongoDB config servers:

```
/etc/init.d/mongod-config1
/etc/init.d/mongod-config2
/etc/init.d/mongod-config3

/etc/mongod-config1.conf
/etc/mongod-config2.conf
/etc/mongod-config3.conf
```

If you're changing the directory path of the config servers, make sure to fix the directory permissions after creating it (`chown mongod:mongod /new/path`).

Turning on the MongoDB config servers can be done by:

```
[root@tsds ~]# service mongod-config1 start
[root@tsds ~]# service mongod-config2 start
[root@tsds ~]# service mongod-config3 start
```

Remember to enable them to start up upon boot:

```
[root@tsds ~]# chkconfig mongod-config1 on
[root@tsds ~]# chkconfig mongod-config2 on
[root@tsds ~]# chkconfig mongod-config3 on
```

## MongoDB mongos Configuration

The [mongos](http://docs.mongodb.org/manual/reference/program/mongos/) utility is the service that clients or applications connect to in a sharded environment.  It functions identically to connecting directly to a `mongod` instance in a non-sharded environment and hides all details involved with talking to multiple servers.  This is typically run on the default mongo port of 27017 so that all defaults "just work" out of the box, though it can be changed if desired.

Example init script and config files should have been installed with the grnoc-tsds-services package for mongos:

```
/etc/init.d/mongos
/etc/mongos.conf
```

Change the `configDB` line as needed to reference the correct location of all config servers running  It is important that all `mongos` instances specify the exact same set of config servers.  Typically only a single `mongos` instance is run per host.  You should also change the hostname from localhost to instead be the actual hostname or public IP address of the server.

Turning on `mongos` can be done by:

```
[root@tsds ~]# service mongos start
```

Remember to enable it to start up upon boot:

```
[root@tsds ~]# chkconfig mongos on
```

You should now be able to connect to `mongos` using just the `mongo` command from the CLI.  If you changed the default ports or anything you will need to specify that on the `mongo` command as well.

```
[root@tsds ~]# mongo
MongoDB shell version: 3.0.7
connecting to: test
mongos>
```

## MongoDB Sharding Configuration

Prior versions of MongoDB had shards which were single threaded, but it appears in 3.0 that shards are able to utilize multiple threads.  Running multiple shards on a server is therefore not needed to take advantage of multiple cores, but it can help performance when migrating data from one shard to another to run multiple shards on a single host in order to reduce the amount of data being transferred.  It can also help with load distribution: a powerful server could run three shards, and a server with less cores or I/O throughput could run only two shards.

Each instance of `mongod` running that is not a config server represents a shard and will store only a subset of all TSDS data.  These shards can be on the same machine or they can be on separate machines, or some combination of the two.  It is generally a good idea to make shards on equivalent machines as MongoDB will attempt to equally balance between all available shards.

How many shards you need is very dependent on the number of things you are collecting on and how powerful the machines are both in terms of IO capacity and storage.  Shards can be added or removed later as necessary so it is not necessary to get it exactly correct the first time.  As a general rule of thumb, we recommend running one shard per host to get started.

Example init scripts and config files should have been installed with the `grnoc-tsds-services` package for a MongoDB shard:

```
/etc/init.d/mongod-shard1

/etc/mongod-shard1.conf
```

If you're changing the directory path of the shards, make sure to fix the directory permissions after creating it (`chown mongod:mongod /new/path`).

Turning on the MongoDB shard servers can be done by:

```
[root@tsds ~]# service mongod-shard1 start
```

Remember to enable them to start up upon boot:

```
[root@tsds ~]# chkconfig mongod-shard1 on
```

All shards need to be added to the cluster and be known by the config servers via `mongos`.  Once again, connecting to `mongos` is done by using the `mongo` CLI client.  Replace with the appropriate hostname and port if the defaults have changed.

```
[root@tsds ~]# mongo
MongoDB shell version: 3.0.7
connecting to: test
mongos> sh.addShard("tsds.grnoc.iu.edu:27025")
{ "shardAdded" : "shard0000", "ok" : 1 }
mongos>
```

## MongoDB Authorization Configuration

Mongo uses something called a "keyFile" to secure authorization between mongod instances. This is really nothing more elaborate than a really long password in a file.  An example of generating a keyfile:

```
[root@tsds ~]# openssl rand -base64 741 > /etc/mongodb-keyfile
```

This file must be used by every instance of `mongod` and `mongos`.  The contents for each must be exactly the same, so copy it to all other servers.  MongoDB actually enforces permissions on this file, so be sure it is read only by the `mongod` user or it might refuse to start with some error in the logs about open permissions:

```
[root@tsds ~]# chown mongod:mongod /etc/mongodb-keyfile
[root@tsds ~]# chmod 600 /etc/mongodb-keyfile
```

The `mongod` config servers and shards should specify the following in their config files:

```
security:
   authorization: "enabled"
   keyFile: "/etc/mongodb-keyfile"
```

For `mongos` instances, it is:

```
security:
   keyFile: "/etc/mongodb-keyfile"
```

All `mongod` config servers and shards, as well as all `mongos` instances must be stopped and restarted for authorization to be enabled.  Once running again, MongoDB runs now in what is called a [localhost exception](https://docs.mongodb.org/manual/core/security-users/#localhost-exception) mode.  Because no admin/root user has been created yet, MongoDB allows you to connect without any user or password specified to give you the opportunity to create one:

```
[root@tsds ~]# mongo admin
MongoDB shell version: 3.0.7
connecting to: admin
mongos> db.createUser({ user: 'root', pwd: 'put password here', roles: [ {role: 'root', db: 'admin'} ] });
```

Verify this was successful by running the following and ensuring it returns this output:

```
mongos> db.getUsers()
[
        {
                "_id" : "admin.root",
                "user" : "root",
                "db" : "admin",
                "roles" : [
                        {
                                "role" : "root",
                                "db" : "admin"
                        }
                ]
        },
mongos> exit
bye
[root@tsds ~]#
```

From now on, when logging into mongo, you will either need to specify the the authenticationDatabase or log into the admin database like so:

```
[root@tsds ~]# mongo -u root admin -p
MongoDB shell version: 3.0.7
Enter password:
connecting to: admin
mongos>
```

## MongoDB SSL Configuration

MongoDB does not do any encryption by default and must be told to do so.  The first step to this is creating X.509 certificates that all of the servers must use to enable TLS communication between them.  Depending on your needs, a self-signed CA should work, and has been tested with TSDS.  One important bit to know that MongoDB imposes is that the CN in each certificate *must* match the hostname that that mongo instance is running on.  The name of the certificate file itself is irrelevant.  Every `mongod` and `mongos` instance running on the same host can share the same .pem file, but ones running on other hosts will require a separate .pem file and a copy of the CA .crt.

The X.509 certificates can be created by doing the following:

```
[root@tsds ~]# certtool -p --outfile /etc/pki/tls/private/mongo-`hostname'.key
[root@tsds ~]# certtool -s --load-privkey /etc/pki/tls/private/mongo-'hostname'.key --outfile /etc/pki/tls/certs/mongo-'hostname'.crt
[root@tsds ~]# cat /etc/pki/tls/certs/mongo-'hostname'.crt >> /etc/pki/tls/private/mongo-'hostname'.key
```
Once again, make sure to specify the proper hostname for the `Common name` option.

File ownership and permissions need to be set appropriately on the certificates:

```
[root@tsds ~]# chown mongod:mongod /etc/pki/tls/certs/mongo-hostname.crt
[root@tsds ~]# chown mongod:mongod /etc/pki/tls/private/mongo-hostname.key
[root@tsds ~]# chmod 400 /etc/pki/tls/certs/mongo-hostname.crt
[root@tsds ~]# chmod 400 /etc/pki/tls/private/mongo-hostname.key
```

Each `mongod` and `mongos` instance will need its corresponding config file updated to have the "net" section look like the following with the correct certificate file paths:

```
net:
  port: <whatever port number was already here>
  ssl:
    mode: "preferSSL"
    CAFile: "/etc/pki/tls/certs/mongo-hostname.crt"
    PEMKeyFile: "/etc/pki/tls/private/mongo-hostname.key"
    clusterFile: "/etc/pki/tls/private/mongo-hostname.key"
    clusterPassword: "password used when creating certs"
```

clusterPassword is un-needed if you generate the certs using the above 'certtool' commands.
If you are using self-signed certificates, you will also need to include the following in the "ssl" section:

```
allowInvalidCertificates: "true"
```

Once again, all `mongod` config servers and shards, as well as all `mongos` instances must be stopped and restarted for SSL to be enabled.

## TSDS Database Bootstrap

Before beginning, make sure to change the mongo password placeholders with passwords of your choosing for the `tsds_ro` and `tsds_rw` users and enter the mongo `root` password for the `root` user in /etc/grnoc/tsds/services/config.xml.  The install script will create the `tsds_ro` and `tsds_rw` users for you with the correct privileges with these passwords.

The `grnoc-tsds-services` package comes with a bootstrapping script that will create, shard, and index a set of
predefined collections based on the set of json files in `/usr/share/doc/grnoc/tsds/install/`.

To perfrom the bootstrap, run the following:

```
[root@tsds ~]# /usr/bin/tsds_install.pl
```

This should create an initial set of example databases / measurement types.  Measurement types can be managed later in the admin section of the TSDS web interface.

## Memcached Installation

[memcached](http://memcached.org) is used by the TSDS writers to keep a cache of knowledge of the documents it has operated on to help avoid further MongoDB database queries.  Any host which will have a set of writers should also have a `memcached` installation they point to.  It is then okay to have multiple `memcached` installation across multiple hosts.  Installating `memcached` can be done by:

```
[root@tsds ~]# yum install memcached
```

Turning on the `memcached` server can be done by:

```
[root@tsds ~]# service memcached start
```

Remember to enable it to start up upon boot:

```
[root@tsds ~]# chkconfig memcached on
```

## Redis Installation

[redis](http://redis.io) is used by the TSDS writers as a distributed lock service when operating on MongoDB documents.  This is needed because MongoDB does not support transactions, and sometimes multiple operations need to be performed on a document in an atomic fashion to prevent other writer processes from potentially overwriting changes and leading to lost updates.

Unlike `memcached` where it is safe to have multiple instances across multiple hosts, **it is not safe to use multiple instances of `redis` for TSDS**.  Only a single centralized `redis` instance should be installed that all TSDS writers point to.  Installing `redis` can be done by:

```
[root@tsds ~]# yum install redis
```

Turning on the `redis` server can be done by:

```
[root@tsds ~]# service redis start
```

Remember to enable it to start up upon boot:

```
[root@tsds ~]# chkconfig redis on
```

## RabbitMQ Installation

[RabbitMQ](https://www.rabbitmq.com) is used by TSDS to act as a messaging queue for incoming updates from external data collectors that need to be updated appropriately in MongoDB.  The TSDS writers continuously read messages off this queue and perform the necessary MongoDB updates.  Typically, only a single `rabbitmq-server` instance should be used on a single host, although more complex installations could make use of more.  Installing RabbitMQ can be done by:

```
[root@tsds ~]# yum install rabbitmq-server
```

The `rabbitmq_management` plugin is required for monitoring and is extremely useful for watching overall health of the rabbit
queue system, so we need to ensure that that is enabled in `/etc/rabbitmq/enabled_plugins`.

```
[root@tsds ~]# cat /etc/rabbitmq/enabled_plugins
[rabbitmq_management].
[root@tsds ~]#
```

Additionally, we will want to make changes to the default `rabbitmq-server` configuration file in `/etc/rabbitmq/rabbitmq.config`:

```
[root@tsds ~]# cat /etc/rabbitmq/rabbitmq.config
 [
    {mnesia, [{dump_log_write_threshold, 1000}]},
    {rabbit, [
         {tcp_listeners, [5672]},
     {hipe_compile, false},
     {vm_memory_high_watermark, 0.75},
     {vm_memory_high_watermark_paging_ratio, 0.75},
     {disk_free_limit, 30000000}
     ]}

 ].
 [root@tsds ~]#
 ```
 
This will allow remote connections to port 5672 so that external hosts where collectors or TSDS writer instances live may communicate to it.  Additionally, we will want to adjust the memory and disk free limit watermarks to be considerably higher than the defaults. Essentially what is going on here is that once rabbit detects that the system has surpassed its watchdog threshold, it will start silently dropping messages in an attempt to reduce its impact on the machine and prevent it from crashing.  The defaults are fairly conservative and the impact is that we lose data, so we want to make sure rabbit is able to aggressively queue messages in the event of an issue.  For more detailed information, see the [RabbitMQ memory docs](http://www.rabbitmq.com/memory.html).

**Note**: You may need to make sure an entry in `/etc/hosts` exists with the proper IP address and hostname in order for `rabbitmq-server` to start successfully.

Turning on the `rabbitmq-server` can be done by:

```
[root@tsds ~]# service rabbitmq-server start
```

Remember to enable it to start up upon boot:

```
[root@tsds ~]# chkconfig rabbitmq-server on
```

## TSDS Writer Configuration

The TSDS writers are reponsible for reading messages from collectors off of the RabbitMQ queue, coordinating with each other using Redis as MongoDB document locks, maintaining their Memcached data to keep track of prior documents they've handled before, and finally making the proper updates to MongoDB accordingly.  The configuration file at `/etc/grnoc/tsds/services/config.xml` contains the information for the writer processes and how to connect to the `rabbitmq-server`.  In a normal setup, the defaults are all fine to leave here but they can be edited as necessary.  Multiple writers may live on multiple hosts as necessary in order to be able to keep up with all the incoming messages to the RabbitMQ queue.

Turning on the `tsds_writer` can be done by:

```
[root@tsds ~]# service tsds_writer start
```

Remember to enable it to start up upon boot:

```
[root@tsds ~]# chkconfig tsds_writer on
```

## Apache Configuration

The `grnoc-tsds-services` package comes with a default apache configuration file in `/etc/httpd/conf.d/grnoc/tsds-services.conf`.  This file must be `Include`d in the Apache configuration so that the `Location`s are available.  The package provides no authentication configuration of its own, so if you want to add something like basic authentication or your insitution's SSO, you will need to add the relevant bits to the config file.

If you are running both the frontend and the services on the same machine, ensure that `tsds-services.conf` is included *BEFORE* the frontend's configuration as they use the same location prefix and may conflict with each other otherwise.

In addition, to enable server-side graph generation, the following steps are required:

- Another apache configuration file, `/etc/httpd/conf.d/grnoc/tsds-services-temp.conf`, must be configured so that the web path `/tsds-services/temp` is defined.  It's very important that this location must be accessable via HTTP not HTTPS and must not be protected by any authentication systems. For example, the file myfile.html in this location must be accessed using the URL: `http://host.com/tsds-services/temp/myfile.html` directly.
- GLUE and YUI must also be accessible via HTTP in addition to the standard HTTPS

To do this, `/etc/httpd/conf/httpd.conf` should include the following (notice that all the Rewrite rules should be commented or removed):

```
<VirtualHost *:80>
        #RewriteEngine On
        #RewriteCond %{HTTPS} off
        #RewriteRule (.*) https://%{HTTP_HOST}%{REQUEST_URI} [R,L]

        INCLUDE conf.d/grnoc/yui.conf
        INCLUDE conf.d/grnoc/glue.conf
        INCLUDE conf.d/grnoc/tsds-services-temp.conf
</VirtualHost>
```

Turning on `httpd` can be done by:

```
[root@tsds ~]# service httpd start
```

Remember to enable it to start up upon boot:

```
[root@tsds ~]# chkconfig httpd on
```

## TSDS Aggregate Configuration

By default, the TSDS bootstrap sets up measurement types with a predefined set of information on how to aggregate data up to lower resolutions and how to expire old data  These defaults are extremely liberal and may need to be adjusted.  The expiration time primarily impacts on how much disk space will be utilized--the longer you keep data around, the more
disk space it takes up.  The aggregation windows help to make queries more efficient at larger time resolutions - being able to utilize one-hour pre-calculated averages makes a month query much faster than using 10-second raw samples.

A separate `grnoc-tsds-aggregate` package provides two tools, one that finds data that needs to be aggregated, and another which performs the aggregation work and sends it to a separate queue to be processed by the writer which is part of this package.  It will need to be installed and configured as well.

## Sphinx Search Configuration

TSDS uses [Sphinx](http://sphinxsearch.com) to index the measurement documents for searching.  The `searchd` daemon must be configured to point to the indexer tool `/usr/bin/tsds_search_indexer.pl` installed by `grnoc-tsds-services`:

```
[root@tsds ~]# cp /etc/sphinx/sphinx.conf.tsds /etc/sphinx/sphinx.conf
[root@tsds ~]# /usr/bin/indexer tsds_metadata_index
[root@tsds ~]# /usr/bin/indexer tsds_metadata_delta_index
[root@tsds ~]# service searchd start
```

Enable the delta index and merger in cron by uncommenting them out in `/etc/cron.d/tsds-services.cron`.  Remember to enable it to start up upon boot:

```
[root@tsds ~]# chkconfig searchd on
```
