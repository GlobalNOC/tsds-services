## GRNOC TSDS Services 1.5.0 -- Tue Feb 16 2016

### Features:

* ISSUE=12536 query language now supports relative (from "now") and epoch timestamps in between clause
* ISSUE=12641 MongoDB document writes are now done in bulk operations to improve performance with less round trips
* ISSUE=12401 improved performance for search suggestions using new get_distinct_meta_field_values() webservice method
* ISSUE=12463 set the "updated_start" and "updated_end" timestamps in data documents for new aggregation framework
* ISSUE=12408 support data aggregation within the writer worker processes when receiving aggregate message for new aggregation framework
* ISSUE=12464 aggregate data from other aggregated data when possible (1 day aggregates will use 1 hour aggregate data)

### Bug Fixes:

* ISSUE=12566 properly set eval position for the default aggregation and expiration rules upon new installation
* ISSUE=12412 require data interval to be a positive, non-zero integer
* ISSUE=12499 fixed default file path location for sphinx search index template files
* ISSUE=12500 properly handle unicode characters when indexing sphinx search data


## GRNOC TSDS Services 1.4.2 -- Wed Nov 04 2015

### Features:

 * ISSUE=12142 now open sourced on GitHub: https://github.com/GlobalNOC/tsds-services
 * ISSUE=11649 added ability to set the classifier flag on non hierarchical meta fields
 * ISSUE=12085 added ability to not use SI notation on a per measurement type basis
 * ISSUE=12129 added support for an optional `having` clause in queries to post-filter results
 * ISSUE=12129 added ability to perform math on get fields, such as `average(values.input) - average(values.output)`
 * ISSUE=12129 support arbitrary nested aggregation operations instead of just two layers
 * ISSUE=12131 support ability to search/filter by value
 * ISSUE=12133 added ability for the user to define which fields the query is grouped by in the TSDS browse section's advanced search
 * ISSUE=12135 added ability to only take the first unique pairing when doing a group by, allowing queries to specify uniqueness without
               doubling up data such as in the case of circuits with two endpoints
 * ISSUE=12149 addressed issues in INSTALL doc and converted it to use markdown syntax for GitHub
 * ISSUE=12232 added script to fix overlapping measurement documents
 * ISSUE=12241 added index on "identifier" in measurement collection to improve writer lookups on cache misses
 * ISSUE=12355 added `tsds_find_unsharded.pl` script to help with identifying bootstrapping issues on some collections that have been deployed
 * ISSUE=12362 moved unknown field warning messages down to debug level
 * ISSUE=12315 added --noconfirm option to tsds_upgrade.pl to allow for unattended upgrades if desired. The default is still interactive.

### Bug Fixes:

 * ISSUE=12027 fixed issue where event query for one day didn't include events whose start and end are not on the same date in the result
 * ISSUE=12111 fixed bug where writer would fail when it encountered a measurement type without a valid metadata collection & document
 * ISSUE=12145 removed problematic code that tried to optimize messages with > 100 data points
 * ISSUE=12304 added missing indexes on start and end in measurements collection to upgrade script
 * ISSUE=12319 moved temporary workspace over to a single, shared, sharded collection to prevent a MongoDB deadlock bug
 * ISSUE=12343 fixed problem where aggregate processes were stacking on top of one another ignoring existing lock files
 * ISSUE=12363 fixed issued where aggregate data collections weren't getting sharded and added unit tests to verify necessary collections are


## GRNOC TSDS Services 1.4.1 -- Tue Sep 29 2015

### Bug Fixes: 

 * ISSUE=12126 Fixed issue where the search API was not respecting the order of the ordinal fields set on value fields
 * ISSUE=11369 add missing last_updated index to measurements collection


## GRNOC TSDS Services 1.4.0 -- Fri Sep 25 2015

### Features:

 * ISSUE=10785 added new metadata management webservice API that will be used in a future frontend release
 * ISSUE=12026 allow configuration for sphinx search instances that reside on remote servers
 * ISSUE=11900 several INSTALL documentation updates and improvements
 * ISSUE=11495 added support for different constrained views of the data configured at separate URLs
 * ISSUE=11951 changed default parallel processes used when aggregating data from 32 to 4
 * ISSUE=11369 added new global search API using sphinx for backend indexing
 * ISSUE=11801 global search query support from browse section

### Bug Fixes:

 * ISSUE=11556 data aggregation and expiration rules are now maintained separately
 * ISSUE=11793 added support for angle, square, and curly brackets in the query language
 * ISSUE=12042 fixed issue where search section was only sorting the current page
 * ISSUE=12060 provide index hints for MongoDB queries to try and address performance problems
 * ISSUE=12019 only allow lowercase a-z and underscore characters for measurement type and meta field names
 * ISSUE=11936 writer will now re-activate decom'd measurements
 * ISSUE=11896 greatly improved performance by not processing existing data points of a document being updated
 * ISSUE=11881 fix bug where we were holding onto all the memory of every document being updated in a single message update
 * ISSUE=11814 fix bug to be able to clear ordinal field on value types
 * ISSUE=11802 continuously attempt to reconnect to rabbit in case of connection failure
 * ISSUE=12091 fixed issue where queries with a limit and a sort may not be returned in the proper order
 * ISSUE=12102 make sure values stored in mongo don't get stringified


## GRNOC TSDS Services 1.3.0 -- Wed Aug 05 2015

### Features:

 * ISSUE=11681 Added authentication support. This change is not backwards compatible with
existing clusters running in no-auth mode and requires upgrading them. The upgrade script
*must* only be run after authentication/authorization has been enabled and a root user has
 been created.
 * ISSUE=11495 added initial constrained/custom view support
 * ISSUE=11368 improved bootstrap/install unit tests
 * ISSUE=9627 starting working on global search support (not yet available)

### Bug Fixes:

 * ISSUE=11635 fall through to use aggregate data if no high res docs exist
 * ISSUE=11633 fix document expiration algorithm to match what aggregations do (based upon policy eval position)
 * ISSUE=11281 chkconfig support fix for init script
 * ISSUE=11429 disallow dashes in measurement type names (MongoDB doesn't like them)


## GRNOC TSDS Services 1.2.2 -- Thu Jun 18 2015

### Bug Fixes:

 * ISSUE=11390 allow null end timestamps for events


## GRNOC TSDS Services 1.2.1 -- Tue Jun 09 2015

### Bug Fixes:

 * ISSUE=11281 Fixed typo in tsds_writer init script where chkconfig would not recognize it
 * ISSUE=11290 Improve performance by specifying index hints and not fetching the same high res docs multiple times when aggregating


## GRNOC TSDS Services 1.2.0 -- Wed May 27 2015

### Features:

  * ISSUE=10949 always store 1000 data points inside data documents based upon provided interval instead of 2 hour long documents
  * ISSUE=11070 increase aggregate doc data points from 100 to 1000 and store them as three dimensional arrays
  * ISSUE=11225 added support for aggregation histogram configuration in admin services
  * ISSUE=11080 dont disable journaling in provided config file
  * ISSUE=10805 only retrieve aggregate histogram data when its needed by the query to improve performance
  * ISSUE=11187 use measurement min & max when calculating histogram buckets, or fall back to min_width when doing dynamic calculation if needed
  * ISSUE=10798 added ability for tsds_firehose.pl test script to do best-case, worst-case, and sin-cos import options
  * ISSUE=11007 do full doc merge and replace instead of supplying mass update clauses to mongo when doing large amounts of updates
  * ISSUE=10462 added sorting and pagination for portal results
  * ISSUE=10697 automatically set ordinal values for the required metadata fields when adding new measurement types
  * ISSUE=10708 added ability to set the maximum number of documents and the maximum number of events a query can return

### Bug Fixes:

  * ISSUE=10944 prevent writer from crashing when handling invalid/malformed data
  * ISSUE=10925 fix issue where the writer wasn't requeueing messages to try again later when MongoDB was unavailable
  * ISSUE=11193 fixed issue where the tsds_install bootstrapping script was not properly adding values to measurement types from their .json files
  * ISSUE=10957 uniquely identify events by start time + identifier instead of event text
  * ISSUE=10841 fix pymongo's ConnectionFailure namespace and fixing hard coded rabbitmq host and port in tsds_meta.py script
  * ISSUE=10702 aggregate measurements only once per interval using the last eval_position aggregation rule match
  * ISSUE=10700 fix bug where eval position for aggregate rules was being calculated incorrectly when changed


## GRNOC TSDS Services 1.1.2 -- Tue Mar 31 2015

### Features:

  * ISSUE=10703 Moved TSDS SNAPP Migrator into its own package out of grnoc-tsds-services

### Bug Fixes: 

  * ISSUE=10701 fix tsds_expire.pl script


## GRNOC TSDS Services 1.1.1 -- Thu Mar 26 2015

### Bug Fixes: 

  * ISSUE=10691 Including missing aggregation.cgi and admin.cgi files


## GRNOC TSDS Services 1.1.0 -- Tue Mar 24 2015

### Features:

  * ISSUE=7992 support migrating data from old SNAPP
  * ISSUE=9992 added temperature bootstrapping data to install
  * ISSUE=10141 added admin section webservice support
  * ISSUE=10254 added server side graph support
  * ISSUE=10353 support data interval changes
  * ISSUE=10356 improved init script for perl tsds_writer
  * ISSUE=10520 added backend changes for data retention methods
  * ISSUE=10528 added event support in perl tsds_writer

### Bug Fixes:

  * ISSUE=9951 properly handle case when no measurements exist when aggregating data
  * ISSUE=10429 improve parallelization for data aggregation, lock file support, improved performance, and fixed file permissions
  * ISSUE=10430 fix aggregate data fall through support issues and improved performance
  * ISSUE=10495 Added "alternate_intf" to store the other form of an interface's name, such as "Te0/0/0" versus "TenGigabitEthernet0/0/0"
  * ISSUE=10631 Fixed issue with fetching values for metadata fields that have a _ in them when using logic parameters, such as "alternate_intf_like"
  * ISSUE=10525 fix problem where _execute_mongo() got confused with any warning messages from the mongo CLI


## GRNOC TSDS Services 1.0.1 -- Wed Feb 25 2015

### Features:
  * ISSUE=10247 Added generalized support for "events" in TSDS in both the writer processes
and in the query parsing engine.
  * ISSUE=10355 Added aggregation and expiration policy definition webservice support.
  * ISSUE=10254 Initial version of server side get_chart method to allow rendering of charts on the server instead of the client.

### Bug Fixes:
  * ISSUE=10022 Fix bootstrap issues with new versions of MongoDB 3.0
  * ISSUE=10141 Changed collection existence check to use listCollections for performance reasons.
  * ISSUE=10201 Fixed writer processers to attempt to auto reconnect to MongoDB instead of dieing
  * ISSUE=10429 Fix issue where aggregator would attempt to issue a blank bulk query if nothing to do for a document and generate errors.
  

## GRNOC TSDS Services 1.0.0 -- Fri Nov 15 2014

### Features:
 * Initial release of TSDS Services. This package provides a Domain Specific Language and REST interface
to data stored in a MongoDB timeseries database. It allows for pushing data into the system as well as retrieval
of data out. It also includes scripts to help manage aggregation and expiration of the data as well as
a script to read messages from RabbitMQ for data insertion.
