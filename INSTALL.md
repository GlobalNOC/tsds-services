# TSDS Services & MongoDB Install Guide

This document covers installing MongoDB in a sharded environment, along with setting up TSDS services on top of it. The steps given below should be followed in the order they are listed unless you are sure you know what you are doing.  This document also assumes a RedHat Linux environment, or one of its derivatives.

An important note about firewalls: in a sharded environment, every shard needs access to every other shard, as well as all of the config servers. This means that if you are using a firewall such as iptables, which is typically turned on and filtering traffic by default, then each participating machine will need to open all of its mongo related ports (except mongos) to every
other participating machine in order for MongoDB to work properly.

## MongoDB Installation

Adminstering a MongoDB cluster is much more involved than running many other databases.  It is strongly recommend to familiarize yourself with how MongoDB works, in particular about [sharding](https://docs.mongodb.org/manual/core/sharding-introduction/#sharding-introduction) across multiple machines, and also about redundancy via [replica sets](https://docs.mongodb.org/manual/core/replication-introduction/).  This guide will attempt to walk you through the steps necessary to stand up single-shard MongoDB from scratch, but knowledge about MongoDB will likely be required in order to successfully administer a production TSDS installation.

To install the MongoDB RPM packages, first you will need to add the yum repository that MongoDB provides to your system in order for it to do so.  Follow the docs on installing the [MongoDB Yum Repository](http://docs.mongodb.org/manual/tutorial/install-mongodb-on-red-hat-centos-or-fedora-linux/) for RedHat, CentOS, or Fedora Linux.  Afterward, You should be able to do the following:

```
sudo yum install mongodb-org
```

This will install all mongodb-related components, including the mongod server as well as CLI based tools.

## MongoDB Config Server Configuration

In a sharded environment, MongoDB requires what are called [config servers](http://docs.mongodb.org/manual/core/sharded-cluster-config-servers/) to manage everything. These are just `mongod` instances that contain metadata about the sharding environment and not the actual user data stored in the database.  MongoDB typically recommends running three config servers on separate hosts for redundancy purposes in a production environment.  At least one config server must be available.

Example init scripts and config files should have been installed with the `grnoc-tsds-services` package for the MongoDB config servers:

/etc/init.d/mongod-config1
/etc/init.d/mongod-config2
/etc/init.d/mongod-config3

/etc/mongod-config1.conf
/etc/mongod-config2.conf
/etc/mongod-config3.conf

If you're changing the directory path of the config servers, make sure to fix the directory permissions after creating it (`chown mongod:mongod ...`).

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

## MongoDB "mongos" Configuration

The [mongos](http://docs.mongodb.org/manual/reference/program/mongos/) utility is the service that clients or applications connect to in a sharded environment.  It functions identically to connecting directly to a `mongod` instance in a non-sharded environment and hides all details involved with talking to multiple servers.  This is typically run on the default mongo port of 27017 so that all defaults "just work" out of the box, though it can be changed if desired.

Example init script and config files should have been installed with the grnoc-tsds-services package for mongos:

/etc/init.d/mongos
/etc/mongos.conf

Change the `configDB` line as needed to reference the correct location of all config servers running  It is important that all `mongos` instances specify the exact same set of config servers.  Typically only a single `mongos` instance is run per host.  ** You should also change the hostname from localhost to instead be the actual hostname or public IP address of the server.  **

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

How many shards you need is very dependent on the number of things you are collecting on and how powerful the machines are both in terms of IO capacity and storage.  Shards can be added or removed later as necessary so it is not necessary to get it exactly correct the first time.  As a general rule of thumb, we recommend running three shards per host to help facilitate shard balancing in the future.

Example init scripts and config files should have been installed with the `grnoc-tsds-services` package for a MongoDB shard:

/etc/init.d/mongod-shard1
/etc/init.d/mongod-shard2
/etc/init.d/mongod-shard3

/etc/mongod-shard1.conf
/etc/mongod-shard2.conf
/etc/mongod-shard3.conf

If you're changing the directory path of the shards, make sure to fix the directory permissions after creating it (`chown mongod:mongod ...`).

Turning on the MongoDB shard servers can be done by:

```
[root@tsds ~]# service mongod-shard1 start
[root@tsds ~]# service mongod-shard2 start
[root@tsds ~]# service mongod-shard3 start
```

Remember to enable them to start up upon boot:

```
[root@tsds ~]# chkconfig mongod-shard1 on
[root@tsds ~]# chkconfig mongod-shard2 on
[root@tsds ~]# chkconfig mongod-shard3 on
```

All shards need to be added to the cluster and be known by the config servers via `mongos`.  Once again, connecting to `mongos` is done by using the `mongo` CLI client.  Replace with the appropriate hostname and port if the defaults have changed.

```
[root@tsds ~]# mongo
MongoDB shell version: 3.0.7
connecting to: test
mongos> sh.addShard("tsds.grnoc.iu.edu:27025")
{ "shardAdded" : "shard0000", "ok" : 1 }
mongos> sh.addShard("tsds.grnoc.iu.edu:27026")
{ "shardAdded" : "shard0001", "ok" : 1 }
mongos> sh.addShard("tsds.grnoc.iu.edu:27027")
{ "shardAdded" : "shard0002", "ok" : 1 }
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
   keyFile: "/path/to/mongodb/keyfile"
```

For `mongos` instances, it is:

```
security:
   keyFile: "/path/to/mongodb/keyfile"
```

All `mongod` config servers and shards, as well as all `mongos` instances must be stopped and restarted for authorization to be enabled.  Once running again, MongoDB runs now in what is called a [localhost exception](https://docs.mongodb.org/manual/core/security-users/#localhost-exception) mode.  Because no admin/root user has been created yet, MongoDB allows you to connect without any user or password specified to give you the opportunity to create one:

```
[root@tsds ~]# mongo
MongoDB shell version: 3.0.7
connecting to: test
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

**From now on, when logging into mongo, you will either need to specify the the authenticationDatabase or log into the admin database like so**:

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
[root@tsds ~]# openssl genrsa 2048 > /etc/mongodca-key.pem
[root@tsds ~]# openssl req -new -x509 -nodes -days 3650 -key ca-key.pem -out ca-cert.pem
[root@tsds ~]# openssl req -newkey rsa:2048 -days 3650 -nodes -keyout server-key.pem -out server-req.pem
[root@tsds ~]# openssl x509 -req -in server-req.pem -days 3650 -CA ca-cert.pem -CAkey ca-key.pem -set_serial 01 -out server-cert.pem
```

This will generate certificates valid for 10 years.

Each mongo instance will needs its corresponding config file updated to have the "net" section look like the following:

```
net:
  port: <whatever port number was already here>
  ssl:
    mode: "preferSSL"
    CAFile: "/path/to/CA.crt"
    PEMKeyFile: "/path/to/file.pem"
    clusterFile: "/path/to/file.pem"
    clusterPassword: "<password on CA file>"
```

If you are using self-signed certificates, you will also need to include:

```
allowInvalidCertificates: "true"
```

In the ssl section above.

If performing an upgrade from an existing mongo stack, every piece will need to be stopped and this applied, then every piece restarted. It might be useful to do the next step about Authorization while the servers are all down, but it might also be useful to limit the moving pieces per step.

It is strongly recommended that the .pem and .crt files be read only by the mongod user.


Bootstrap the TSDS Database
---------------------------

### IMPORTANT ####
Before running the install script, change the mongo password placeholders with passwords of your choosing for the tsds_ro and tsds_rw users and enter the mongo root password for the root user in /etc/grnoc/tsds/services/config.xml. (The install script will create the tsds_ro and tsds_rw users for you with the correct privileges)

The `grnoc-tsds-services` package comes with a bootstrapping script that will create, shard, and index a set of
predefined collections based on the set of json files in /usr/share/doc/grnoc/tsds/install/.

This file is located at /usr/bin/tsds_install.pl and should be pretty self explanatory when run. It will output the set
of actions that it is performing and will stop if any report errors.

Addition of new measurement types for the time being needs to be handled by someone very familiar with TSDS. There will be a 
forthcoming admin section and/or utility scripts to help manage this in a future version.

You'll need `memcached`, `redis`, and `rabbitmq-server` too, but they don't *have* to be on the same host. It is recommended for a standard 
installation that they be on the same machine however, and the grnoc-tsds-services packages will install them for you.

The `rabbitmq_management` plugin is required for monitoring and is extremely useful for watching overall health of the rabbit
queue system, so we need to ensure that that is enabled in `/etc/rabbitmq/enabled_plugins`.

Additionally we will want to make a few small changes to the default rabbitmq configuration file in `/etc/rabbitmq/rabbitmq.config`. The first thing to change is the tcp_listeners section which by default will only listen on localhost. This is fine as long as you never want anything outside of this machine to be able to listen to a rabbit queue, but if you ever do or ever might you should change it to the below example. This is not able to be changed once the server is started.

Additionally we will want to adjust the memory and disk free limit watermarks to be considerably higher than the defaults. Essentially what is going on here is that once rabbit detects that the system has surpassed its watchdog threshold it will start silently dropping messages in an attempt to reduce its impact on the machine and prevent it from crashing. The defaults are fairly conservative and the impact is that we lose data, so we want to make sure rabbit is able to aggressively queue messages in the event of an issue.

For more detailed information see: http://www.rabbitmq.com/memory.html

### Configure and start rabbitmq

```bash
[root@np-dev2 ~]# cat /etc/rabbitmq/enabled_plugins
[rabbitmq_management].
[root@np-dev2 ~]# cat /etc/rabbitmq/rabbitmq.config
[
   {mnesia, [{dump_log_write_threshold, 1000}]},
   {rabbit, [
            {tcp_listeners, [5672]},
            {hipe_compile, false},
            {vm_memory_high_watermark, 0.75},
            {vm_memory_high_watermark_paging_ratio, 0.75},
            {disk_free_limit, 3000000000}
            ]}

].
[root@np-dev2 ~]# service rabbitmq-server start
Starting rabbitmq-server: SUCCESS
rabbitmq-server.
[root@np-dev2 ~]# chkconfig rabbitmq-server on
```


### Configure and start memcached, redis, and the writer processes

The tsds_writer processes use memcache as a shared memory source, and redis as a distributed locking system.

Start memcached and redis:

# service memcached start
# service redis start

No configuration changes needed for either, set them to start on boot.

The configuration file at /etc/grnoc/tsds/services/config.xml contains the information for the writer processes and how to connect to RabbitMQ. In a normal setup the defaults are all fine to leave here but they can be edited as necessary.

Ensure that the writers are running and are set to start on boot via their init script in `/etc/init.d/tsds_writer`


Enable Apache Locations
-----------------------

This package comes with a default apache configuration file in /etc/httpd/conf.d/grnoc/tsds-services.conf. This file must be Include'd in 
the apache configuration so that the locations are defined. The package provides no authentication of its own so if you want to add something like basic authentication or your insitution's SSO you may add the relevant bits to the config file.

If you are running both the frontend and the services on the same machine ensure that `tsds-services.conf` is included BEFORE the 
frontend's configuration as they use the same location prefix and may conflict with eachother otherwise.

In addition, to enable server-side graph generation, the following steps are required:

- Another apache configuration file (/etc/httpd/conf.d/grnoc/tsds-services-temp.conf) must be configured so that the web path '/tsds-services/temp' is defined. It's very important that this location must be accessable via HTTP not HTTPS and must not be protected by any authentication systems. For example, the file myfile.html in this location must be accessed using the URL: http://host.com/tsds-services/temp/myfile.html directly.
- GLUE and YUI must also be accessable via HTTP, in addition to the regular way with HTTPS/Cosign.
- To make these happen, /etc/httpd/conf/httpd.conf should include (notice that all the Rewrite rules should be commented or removed):
<VirtualHost *:80>
        #RewriteEngine On
        #RewriteCond %{HTTPS} off
        #RewriteRule (.*) https://%{HTTP_HOST}%{REQUEST_URI} [R,L]

        INCLUDE conf.d/grnoc/yui.conf
        INCLUDE conf.d/grnoc/glue.conf
        INCLUDE conf.d/grnoc/tsds-services-temp.conf
</VirtualHost>


Enable Data Aggregation
-----------------------

By default `grnoc-tsds-services` sets up measurement types with a predefined set of information on how to aggregate 
data up to lower resolutions and how to expire old data. This defaults are extremely liberal and may need to be adjusted. 
The expiration time primarily impacts on how much disk space will be utilized - the longer you keep data around the more
space it takes up. The aggregation windows help to make queries more efficient at larger time resolutions - being able to utilize
1h pre calculated averages makes a month query much faster than using 10s raw samples for examples.

This package puts a file in /etc/cron.d/tsds-services.cron that will run the aggregation and expiration defaults. These entries are 
commented out initially to give people a chance to decide how to use it. If additional aggregation windows besides the default are desired
a person very familiar with TSDS will need to assist with setting those up. Similar to the above Bootstrap section, a forthcoming addition
will help with making this an easier experience.


Enable Meta Manager
-------------------
The `grnoc-tsds-meta-manager` package is a tool which updates measurements in TSDS with additional metadata from external data source,
such as setting the circuit information of a measurement by querying the CDS2 webservice.  Measurements that are no longer have data being
submitted will also get decommissioned so that they no longer display in the UI.  After installing the package:

- edit /etc/grnoc/tsds-meta-manager/config.xml setting the proper MongoDB and CDS credentials
- if needed, add additional type entries to the config for any more databases that will need to be managed
- enable them in cron by uncommenting them out in /etc/cron.d/tsds-meta-manager.cron


Enable Meta Collections
-----------------------

`grnoc-tsds-services` provides a simple script called "tsds_meta.py" that periodically collects information about the timeservices system itself
and stores it like any other measurement. To enable this uncomment the entry in /etc/cron.d/tsds-services.cron and every 5 minutes it should
submit information for the meta measurements such as rabbit queue statistics, collection size, etc.


Enable Search
-------------

TSDS uses Sphinx (http://sphinxsearch.com) to index the measurement documents for searching.  The searchd daemon must be configured to point to
the indexer tool (/usr/bin/tsds_search_indexer.pl) installed by `grnoc-tsds-services`:

# cp /etc/sphinx/sphinx.conf.tsds /etc/sphinx/sphinx.conf
# service searchd start
# /usr/bin/indexer tsds_metadata_index --rotate

- enable the delta index and merger in cron by uncommenting them out in /etc/cron.d/tsds-services.cron
