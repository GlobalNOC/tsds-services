#!/usr/bin/python

import signal
import sys
import pika
import threading
import multiprocessing
import os
import pymongo
import json
import time
import collections
import hashlib
import memcache
import daemon
import setproctitle
import argparse
import logging
import logging.config
import xml.etree.ElementTree as xml

from logging.handlers import SysLogHandler
from pymongo.errors import BulkWriteError

# default config file location
DEFAULT_CONFIG_FILE = '/etc/grnoc/tsds/services/config.xml'

LOGGING_FILE = '/etc/grnoc/tsds/services/receiver_logging.conf'

# how big our documents are when unfragmented
BASE_DOC_SIZE  = 3600 * 2
EVENT_DOC_SIZE = 3600 * 24

# Need to check if this can be stored in log file
#EXPECTED_RECORD = 17300

# Main thread that sets up shared data and starts
# the relevant subprocesses
class Control( threading.Thread ):

    def __init__( self, config_file = DEFAULT_CONFIG_FILE ,expectedrecords=0):

        self.config_file = config_file
        self.id = os.getpid()

        # set process title
        setproctitle.setproctitle( "tsds_receiver" )

        # parse xml config file passed in
        xml_tree    		= xml.parse( self.config_file )
        self.config 		= xml_tree.getroot()

        # parse xml config file options
        self.num_consumers 	  = int( self.config.find( 'num-processes' ).text )
        self.pid_file 		  = self.config.find( 'pid-file' ).text
        self.mongo_host 	  = self.config.find( 'mongo' ).attrib.get( 'host' )
        self.mongo_port 	  = int( self.config.find( 'mongo' ).attrib.get( 'port' ) )
        self.memcache_host 	  = self.config.find( 'memcache' ).attrib.get( 'host' )
        self.memcache_port 	  = self.config.find( 'memcache' ).attrib.get( 'port' )
        self.rabbit_host 	  = self.config.find( 'rabbit' ).attrib.get( 'host' )
        self.rabbit_port 	  = int( self.config.find( 'rabbit' ).attrib.get( 'port' ) )
        self.rabbit_queue 	  = self.config.find( 'rabbit' ).attrib.get( 'queue' )
        self.ignore_databases     = self.config.find( 'ignore-databases' ).findall( 'database' )
        self.ignore_databases     = [database.text for database in self.ignore_databases]
	self.recordcount	  = 1
	self.expectedrecords	  = expectedrecords

        # set up base logging information
        logging.config.fileConfig( LOGGING_FILE )
        self.logger 	  	  = logging.getLogger('tsdsreceiver')

        self.logger.info( "tsds_receiver starting" );

        # write our pid file to disk
        file( self.pid_file, 'w' ).write( str( self.id ) )

        self.threads  		  = []
        self.manager 		  = multiprocessing.Manager()        
        self.lock 		  = multiprocessing.Lock()

        self.types 		  = self.manager.dict()
        self.required_meta_fields = self.manager.dict()
        self.optional_meta_fields = self.manager.dict()
        self.meta_values 	  = self.manager.dict()

        self.client = pymongo.MongoClient( self.mongo_host, self.mongo_port )

        # find available collections and their metadata
        self.__parse_databases()

        self.__start_consumers( self.num_consumers )
        
        # setup a sig TERM handler
        def __sig_handler( signal, frame ):

            # stop the manager process
            self.manager.shutdown()

            # terminate all child processes too
            for t in self.threads:
                t.terminate()

            # remove pid file
            os.unlink( self.pid_file )

            # termine this parent process
            os._exit( 0 )

        # catch a SIG TERM and SIG INT with our handler
        signal.signal( signal.SIGTERM, __sig_handler );
        signal.signal( signal.SIGINT, __sig_handler );

        # dont exit main process unless all child threads have exited
        for t in self.threads:
            t.join()

    def __parse_databases( self ):

        dbs = self.client.database_names()

        for db_name in dbs:

            # skip this database if its in the ignore list or its an internal db
            if db_name in self.ignore_databases or db_name.startswith('_'):
                continue

            self.types[db_name] = 1

            database = self.client[db_name]
                
            metadb   = database.metadata.find_one()

            # make sure we have known metadata for this database, skip it otherwise
            if not metadb:
                continue

            self.logger.debug("Found database %s" % db_name)

            fields = metadb['meta_fields']
            values = dict()

            # allow values to be optional
            if 'values' in metadb.keys():
                
                values = metadb['values']

            # parse meta fields
            for key in fields:

                # is it required?
                if fields[key].get('required'):

                    # initialize 2d dict if not already there
                    if not self.required_meta_fields.has_key( db_name ):
                        self.required_meta_fields[db_name] = dict()

                    old = self.required_meta_fields[db_name]
                    old[key] = 1

                    self.required_meta_fields[db_name] = old

                # must be optional
                else:

                    # initialize 2d dict if not already there
                    if not self.optional_meta_fields.has_key( db_name ):
                        self.optional_meta_fields[db_name] = dict()

                    old = self.optional_meta_fields[db_name]

                    if fields[key].get("fields"):
                        for subfield in fields[key]['fields'].keys():
                            old["%s.%s" % (key, subfield)] = 1 
                    else:
                        old[key] = 1

                    self.optional_meta_fields[db_name] = old

            # initialize 2d dict if not already there
            if not self.meta_values.has_key( db_name ):
                self.meta_values[db_name] = dict()

            # parse meta values
            for key in values:
                old = self.meta_values[db_name]
                old[key] = 1

                self.meta_values[db_name] = old

    def __start_consumers(self, number):
        self.logger.debug("Starting %s data threads" % number)

        for i in range(number):
            t = DataConsumer(i,
                             self.lock, 
                             self.types, 
                             self.required_meta_fields, 
                             self.optional_meta_fields, 
                             self.meta_values,
                             self.memcache_host,
                             self.memcache_port,
                             self.rabbit_host,
                             self.rabbit_port,
                             self.rabbit_queue,
                             self.recordcount,
			     self.expectedrecords)

            self.threads.append(t)
            t.start()

# Process that reads data off of rabbit and writes it to mongo
class DataConsumer(multiprocessing.Process):

    def __init__(self, number, shared_lock, shared_types, 
                 shared_required_meta_fields, shared_optional_meta_fields, 
                 shared_meta_values, memcache_host, memcache_port,
                 rabbit_host, rabbit_port, rabbit_queue,recordcount,expectedrecords):

        multiprocessing.Process.__init__(self)        

        self.id                          = number
        self.shared_lock                 = shared_lock
        self.shared_types                = shared_types
        self.shared_required_meta_fields = shared_required_meta_fields
        self.shared_optional_meta_fields = shared_optional_meta_fields
        self.shared_meta_values          = shared_meta_values
        self.memcache_host               = memcache_host
        self.memcache_port               = memcache_port
        self.memcache                    = memcache.Client( [self.memcache_host + ':' + self.memcache_port], debug = 0 )
        self.rabbit_host                 = rabbit_host
        self.rabbit_port                 = rabbit_port
        self.rabbit_queue                = rabbit_queue
        self.recordcount                 = recordcount
	self.expectedrecords		 = expectedrecords

        logging.config.fileConfig( LOGGING_FILE )

        self.logger                      = logging.getLogger("tsdsreceiver")

        # create local copies to avoid consulting the master
        # over and over in most cases
        self.local_types                 = dict(shared_types.items())
        self.local_required_meta_fields  = dict(shared_required_meta_fields.items()) 
        self.local_optional_meta_fields  = dict(shared_optional_meta_fields.items()) 
        self.local_meta_values           = dict(shared_meta_values.items())
        self.local_cache                 = {}
        
        # # set up our connection to mongo
        self.mongo      		 = pymongo.MongoClient()

    def run(self):
        # connect to rabbit 
        connection   = pika.BlockingConnection( pika.ConnectionParameters( host = self.rabbit_host,
                                                                           port = self.rabbit_port ) )
        self.channel = connection.channel()
        
        self.channel.queue_declare(queue = self.rabbit_queue,
                                   durable = False)

        self.channel.basic_qos(prefetch_count = 10)

        self.logger.debug(" %s waiting for input" % self.id)

       	self.channel.basic_consume(self.data_callback,
        	                   queue = self.rabbit_queue,
                	           no_ack = False)
	self.channel.start_consuming() 
    

    def data_callback(self, ch, method, properties, body):

	#print "\n called callback function with record count ",self.recordcount
       
	# parse json message
        try:
            data = json.loads(body)
        except ValueError:
            self.logger.error("Couldn't decode \"%s\" as JSON, skipping" % body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        timing_start = time.time()

        if not isinstance(data, list):
            self.logger.error("Data is not a list, ignoring message")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return            

        self.cache_misses = 0

        try:
            # generate a set of updates and inserts from all the data points
            updates = self.process_data(data)
            
            # send bulk updates over to the mongo server
            self._send_bulk_updates(updates)
        except pymongo.errors.AutoReconnect:
            self.logger.info("Caught AutoReconnect exception, sending message back to rabbit for reprocessing")
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
            return

        # send ack back to rabbit to let it know we're done with this message
        ch.basic_ack(delivery_tag=method.delivery_tag)

        duration = time.time() - timing_start
        self.logger.debug( "[%s] Duration was %.5f for %4i updates (cache misses = %4i, %.2f %%)" % (int(time.time()),
                                                                                                     duration, 
                                                                                                     len(data), 
                                                                                                     self.cache_misses,
                                                                                                     self.cache_misses / float(len(data)) * 100
                                                                                                     ))
	if( self.recordcount > self.expectedrecords and self.expectedrecords != 0 ):
		#print "Processed all records expected"
		sys.exit(1)


    def process_data(self, data):
        processed = {}
        processed_events = {}

        for point in data:

            if not isinstance(point, dict):
                self.logger.error("Datapoint is not a dict, skipping")
                continue

            ptype    = point.get('type')

            if not ptype or (not isinstance(ptype, str) and not isinstance(ptype, unicode)):
                self.logger.error("Datapoint is missing \"type\" or is not str, skipping")
                continue

            is_event = 0
            if (ptype.endswith(".event")):
                is_event = 1
                ptype = ptype.split(".event")[0]

            values   = point.get('values')
            interval = point.get('interval')
            meta     = point.get('meta')
            ptime    = point.get('time')                

            ev_affected = point.get('affected')
            ev_text     = point.get('text')
            ev_start    = point.get('start')
            ev_end      = point.get('end')
            ev_type     = point.get('event_type')


            # basic sanity checks on data structure for event
            if is_event:
                if not ev_affected or not isinstance(ev_affected, dict):
                    self.logger.error("Event datapoint is missing \"affected\" or is not dict, skipping")
                    continue
                if not ev_start or not ("%s" % ev_start).isdigit():
                    self.logger.error("Event datapoint is missing \"start\" or is not digit, skipping")
                    continue
                if not ev_type or (not isinstance(ev_type, str) and not isinstance(ev_type, unicode)):
                    self.logger.error("Event datapoint is missing \"event_type\" or is not string, skipping")
                    continue
                # end can be null for ongoing event or epoch timestamp
                if not point.has_key("end") or (ev_end != None and not ("%s" % ev_start).isdigit()):
                    self.logger.error("Event datapoint is missing \"end\" or is not digit/null, skipping")
                    continue
                if not ev_text or (not isinstance(ev_text, str) and not isinstance(ev_text, unicode)):
                    self.logger.error("Event datapoint is missing \"text\" or is not string, skipping")
                    continue

            # sanity checks for data
            else:
                if not values or not isinstance(values, dict):
                    self.logger.error("Datapoint is missing \"values\" or is not dict, skipping")
                    continue
                
                if not interval or not ("%s" % interval).isdigit():
                    self.logger.error("Datapoint is missing \"interval\" or is not digit, skipping")
                    continue
            
                if not meta or not isinstance(meta, dict):
                    self.logger.error("Datapoint is missing \"meta\" or is not dict, skipping")
                    continue
            
                if ptime == None or not ("%s" % ptime).isdigit():
                    self.logger.error("Datapoint is missing \"time\" or is not integer, skipping")
                    continue
                
                # make sure it's a number
                interval = int(interval)
                point['interval'] = interval

                # can't store 7s interval data in a 60s bucket
                if BASE_DOC_SIZE % interval != 0:
                    self.logger.error("doc size of %s not evenly divisible by data interval %s, skipping" % (BASE_DOC_SIZE, interval))
                    continue



            # get mongo database instances based upon data type
            mongodb                 = self.mongo[ptype]
            data_collection         = mongodb['data']
            measurements_collection = mongodb['measurements']
            metadata_collection     = mongodb['metadata']
            event_collection        = mongodb['event']


            # let's make sure this type of measurement is configured
            if not self.local_types.has_key(ptype):
                if not self.shared_types.has_key(ptype):
                    self.logger.error("Unknown type of data \"%s\", skipping" % ptype)
                    continue
                self.local_types[ptype] = self.shared_types[ptype]

            if is_event:
                valid = self._validate_event_meta_fields( ptype, ev_affected )

                if not valid:
                    continue

                self._update_event(event_collection, 
                                   ev_start,
                                   ev_end, 
                                   ev_type,
                                   ev_text,
                                   ev_affected)
                

            else:
                # validate & determine all required meta fields for this type of data
                required = self._validate_required_meta_fields( ptype, meta )

                if not required:
                    continue

                # calculate unique identifier of the measurement based upon the required metadata fields
                measurement_identifier = self._get_measurement_identifier( required, meta )

                cached_doc = self._get_cache_document_values(ptype, measurement_identifier)

                # add this measurement entry if we've never seen it before
                if not cached_doc:
                    self.logger.debug("Couldn't find %s in cache, attempting to create if necessary" % measurement_identifier)
                    self._add_measurement( measurement_identifier, ptime, meta, measurements_collection )

                # automatically add any new value type we've never seen before to metadata
                self._update_metadata_values( metadata_collection, ptype, values )

                # add any updates necessary into the processed dict
                self._generate_queries( measurement_identifier, point, values, processed )
            
                # see if this update contains any new values from those that already
                # exist in the document
                self._update_new_values( measurement_identifier, point, values, cached_doc )

        return processed

    def _update_event(self, collection, start, end, event_type, text, affected):

        # make sure they're numbers
        start = int(start)
        
        if end != None:
            end = int(end)

        aligned_start = int(start / float(EVENT_DOC_SIZE)) * EVENT_DOC_SIZE

        doc = collection.find_one({"start": aligned_start,
                                   "type": event_type})

        if not doc:
            self.shared_lock.acquire()

            doc = collection.find_one({"start": aligned_start,
                                       "type": event_type})
            
            if not doc:
                doc_id = collection.insert({"start": aligned_start,
                                            "end": aligned_start + EVENT_DOC_SIZE,
                                            "last_event_end": aligned_start + EVENT_DOC_SIZE,
                                            "type": event_type,
                                            "events": []
                                            })
            else:
                doc_id = doc["_id"]

            self.shared_lock.release()
 
        else:
            doc_id = doc["_id"]


        # have to convert "circuit.name" into "circuit => name" for proper
        # storage in Mongo
        parsed_affected = {}

        for key in affected.keys():
            pieces  = key.split(".")
            current = parsed_affected
            for piece in pieces[:-1]:
                current[piece] = current.get(piece, {})
                current = current[piece]
            current[pieces[-1]] = affected[key]


        # We can do this entirely in Mongo where we query an array field and then
        # use the $ positional updater to set the array item that matched
        res = collection.update({'_id': doc_id, 
                                 "events.start": start,
                                 "events.text": text,
                                 "events.affected": parsed_affected
                                 },
                                {"$set": {"events.$.end": end}}
                                )

        self.recordcount += 1                

        # If we didn't match anything just based on start+text+affected
        # we have a new event so insert it
        if res["updatedExisting"] != True:
            res = collection.update({"_id": doc_id},
                                    {'$addToSet': {"events": {"start": start,
                                                              "text": text,
                                                              "end": end,
                                                              "affected": parsed_affected}
                                                   }
                                     })


    # generate a bulk operation for each collection type we're updating and
    # then send when we're finished
    def _send_bulk_updates(self, operations):
	#print "\n Called Updates with record count ",self.recordcount
        for ptype in operations.keys():            
            count     = 0
            bulk_op   = self.mongo[ptype]['data'].initialize_unordered_bulk_op()

            for identifier in operations[ptype].keys():
                for start in operations[ptype][identifier].keys():
                    for end in operations[ptype][identifier][start].keys():

                        count            += 1
        		self.recordcount += 1                
                        updates = operations[ptype][identifier][start][end]['updates']

                        # make sure our object stays in the right order
                        query = collections.OrderedDict()        

                        query['identifier'] = identifier
                        query['start']      = start
                        query['end']        = end           

                        # This is nonobvious but there's no good way to atomically do this due to mongo
                        # not supporting $set and $setOnInsert in the same update if any fields are shared
                        # Basically we try to do the dumb optimistic update and hope it's going to work
                        bulk_op.find(query).update_one({'$set': updates})


            # If we wind up with nMatched < our number of updates, go into the slower update
            # where we try again but this time using upserts with new docs
            try:

                result = bulk_op.execute()           
	    
                if result['nMatched'] != count:

                    self.logger.debug("Doing upsert ops, count was %s but matched was only %s" % (count, result['nMatched']))
                    self._send_bulk_upserts(ptype, operations[ptype])

            except BulkWriteError as bwe:
                
                print bwe.details
                self.logger.error(bwe.details)


    def _send_bulk_upserts(self, ptype, operations):
        # ordered_bulk_op is very important here, upsert MUST happen before update 
        # to ensure guarding against a race condition
        upsert_bulk = self.mongo[ptype]['data'].initialize_ordered_bulk_op()        

        for identifier in operations.keys():
            for start in operations[identifier].keys():
                for end in operations[identifier][start].keys():

                    updates = operations[identifier][start][end]['updates']

                    # make sure our object stays in the right order
                    query = collections.OrderedDict()        
                    
                    query['identifier'] = identifier
                    query['start']      = start
                    query['end']        = end           

                    doc_info = self.local_cache['%s%s' % (ptype, identifier)]

                    new_values = self._generate_data_structure(doc_info)

                    new_doc =  {
                        'identifier': identifier,
                        'start': start,
                        'end'  : end,
                        'updated': int(time.time()),
                        'interval': doc_info['interval'],                    
                        'values': new_values
                        }           

                    upsert_bulk.find(query).upsert().update_one({'$setOnInsert': new_doc})
                    upsert_bulk.find(query).update_one({'$set': updates})

                    # cache the value types that this new document supports for later, if necessary
                    self._cache_document_values( ptype, 
                                                 identifier, 
                                                 start, 
                                                 end,
                                                 doc_info['interval'],
                                                 dict((key, 1) for key in new_values.keys()) 
                                                 )
        try:

            upsert_bulk.execute()

        except BulkWriteError as bwe:
            
            print bwe.details
            self.logger.error(bwe.details)


    # validate that an event's affected fields actually map
    # to the metadata for this type
    def _validate_event_meta_fields( self, type, provided ):
        for name in provided.keys():

            if not isinstance(provided[name], list):
                self.logger.error("Values for affected \"%s\" was not a list, skipping" % name)
                return 0
            
            all_meta = dict(self.local_required_meta_fields[type].items() +
                            self.local_optional_meta_fields.get(type, {}).items())

            if not all_meta.has_key(name):
                self.logger.error("Unknown event affected field \"%s\"" % name)
                return 0                

        return 1

    def _validate_required_meta_fields( self, type, provided ):

        # array to find/store all required fields for this type
        required = []

        # get all required fields for this type
        required = self.local_required_meta_fields[type].keys()

        # make sure every required field was provided
        for key in required:

            # did they not provide a required field?
            if not provided.has_key( key ) or not provided[key]:
                self.logger.error("Data point must have non-empty required field \"%s\" for type %s, skipping" % (key, type))
                return False

        # make sure the required fields are always in the same order
        # so that we consistently hash them the same way
        required.sort()

        return required


    def _get_measurement_identifier( self, required, meta ):
        # generate a unique sha256 hash based upon all the required metafields
        sha = hashlib.sha256()

        for field in required:
            sha.update( meta[field] )

        return sha.hexdigest()
        

    def _add_measurement( self, identifier, timestamp, meta, measurement_collection ):                      

        doc = measurement_collection.find_one({'identifier': identifier})

        # identifier already exists in this measurement collection
        # so there's nothing to do
        if doc:
            return

        self.shared_lock.acquire()

        # try to find it again to make sure in our time to lock 
        # someone else didn't sneak in and do it
        doc = measurement_collection.find_one({'identifier': identifier})

        if not doc:

            self.logger.debug("Creating new doc for %s" % identifier)
            
            new_doc = {'identifier': identifier,
                       'start': timestamp,
                       'end': None
                       }
            
            res = measurement_collection.insert(dict(new_doc.items()
                                                     +
                                                     meta.items()
                                                     ))

        self.shared_lock.release()


    def _update_metadata_values( self, metadata_collection, ptype, values ):

        # automatically add any new value type we've never seen before to metadata
        for key in values.keys():

            # we don't currently know about this value type
            if not self.local_meta_values[ptype].has_key( key ):

                # make sure only one process adds the new type
                self.shared_lock.acquire()

                # make sure another process didn't add it already
                if not self.shared_meta_values[ptype].has_key( key ):
                    
                    self.logger.info("Adding new value type \"%s\" to collection type \"%s\"" % (key, ptype))
                    
                    # update the document array structure
                    
                    # update the mongodb metadata collection entry too
                    to_set = collections.OrderedDict()

                    to_set['values.%s' % key] = {'description': key,
                                                 'units': key}
                
                    result = metadata_collection.update({}, {'$set': to_set}, multi=False)

                    old = self.shared_meta_values[ptype]
                    old[key] = 1

                    self.shared_meta_values[ptype] = old

                # copy shared cache value to our local cache too
                self.local_meta_values[ptype][key] = self.shared_meta_values[ptype][key]

                # all done with this lock
                self.shared_lock.release()


    def _get_datapoint_document_info( self, datapoint ):

        ptype    = datapoint.get('type')
        interval = datapoint.get('interval')
        ptime    = datapoint.get('time')
        values   = datapoint.get('values')

        size = BASE_DOC_SIZE / interval
        dimensions = 3

        # declare these so that they're all available later
        # even if we didn't wind up using them
        size_x = size_y = size_z = None
        index_z = index_y = index_z = None

        # if we're doing 10 minute or slower docs, use a single
        # dimension to avoid lots of unnecessarily small arrays
        if interval >= 600:

            dimensions = 1
            size_x = size

        else:

            # best guess here is to figure out what the cube root of the
            # BASE_DOC_SIZE / interval is and round up
            factors = prime_factors(size)

            # provided interval isn't a factor of 3
            if (len(factors) != 3):
                self.logger.critical("Interval is %s" % interval)
                self.logger.critical("Factors were %s" % factors)
                sys.exit(1)

            size_x, size_y, size_z = factors

            # dumb sanity check to make sure that our calculations work
            assert size_x * size_y * size_z == size

        # align point onto interval mark
        ptime = int(ptime / float(interval)) * interval

        # determine start and end timestamp of document
        start = int(ptime / float(BASE_DOC_SIZE)) * BASE_DOC_SIZE
        end   = start + BASE_DOC_SIZE

        # determine x, y, z index of this data pooint
        time_diff = (ptime - start) / interval

        # 1d
        if dimensions == 1:
            index_x = time_diff

        # 3d
        else:
            index_x   = time_diff / (size_y * size_z)
            remainder = time_diff - (size_y * size_z * index_x)
            index_y   = remainder / (size_z)
            index_z   = remainder % size_z

        # return all the calculated document info for this data point
        return {
            'dimensions': dimensions,
            'size_x': size_x,
            'size_y': size_y,
            'size_z': size_z,
            'index_x': index_x,
            'index_y': index_y,
            'index_z': index_z,
            'start': start,
            'end': end,
            'values': values,
            'interval': interval
            }

    def _generate_queries(self, measurement_identifier, point, values, processed):

        interval        = int(point['interval'])
        ptype           = point['type']
        mongodb         = self.mongo[ptype]
        data_collection = mongodb['data']

        # determine attributes of the document for this datapoint
        doc_info = self._get_datapoint_document_info( point )

        start      = doc_info['start']
        end        = doc_info['end']
        dimensions = doc_info['dimensions']
        index_x    = doc_info['index_x']
        index_y    = doc_info['index_y']
        index_z    = doc_info['index_z']
        size_x     = doc_info['size_x']
        size_y     = doc_info['size_y']
        size_z     = doc_info['size_z']

        self.local_cache['%s%s' % (ptype, measurement_identifier)] = doc_info

        # make sure we have everything in place for all operations
        # to this type, identifier, and start/end
        if not processed.has_key(ptype):
            processed[ptype] = {}
            
        if not processed[ptype].has_key(measurement_identifier):
            processed[ptype][measurement_identifier] = {}

        if not processed[ptype][measurement_identifier].has_key(start):
            processed[ptype][measurement_identifier][start] = {}

        if not processed[ptype][measurement_identifier][start].has_key(end):
            processed[ptype][measurement_identifier][start][end] = {}

        if not processed[ptype][measurement_identifier][start][end].has_key('updates'):
            processed[ptype][measurement_identifier][start][end]['updates'] = {}

        updates = processed[ptype][measurement_identifier][start][end]['updates']

        for key in values.keys():
        
            # don't need to set nulls, they're already there
            if values[key] == None:
                continue

            if dimensions == 1:
                updates['values.%s.%s' % (key, index_x)] = values[key]

            elif dimensions == 3:
                updates['values.%s.%s.%s.%s' % (key, index_x, index_y, index_z)] = values[key]


        updates['updated'] = int(time.time())


    def _cache_document_values(self, ptype, measurement_identifier, start, end, interval, values):
        cached = {'start': start,
                  'end': end,
                  'interval': interval,
                  'values': values
                  }

        self.memcache.set('%s%s' % (str(ptype), str(measurement_identifier)), cached)

        return cached

    def _get_cache_document_values(self, ptype, measurement_identifier):
        res = self.memcache.get('%s%s' % (str(ptype), str(measurement_identifier)))

        if res:
            return res

        return {}


    def _update_new_values(self, measurement_identifier, point, values, cached_doc):
        ptype = point['type']

        doc_info   = self.local_cache['%s%s' % (ptype, measurement_identifier)]
        dimensions = doc_info['dimensions']
        size_x     = doc_info['size_x']
        size_y     = doc_info['size_y']
        size_z     = doc_info['size_z']
        start      = doc_info['start']
        end        = doc_info['end']

        cached_start = cached_doc.get('start')
        cached_end   = cached_doc.get('end')

        # if this update isn't to the same doc as we last knew about,
        # we need to find the doc
        if cached_start != start or cached_end != end:
            # blow away old cached value
            self.cache_misses += 1

            # try to find doc that this is updating
            doc = self.mongo[ptype]['data'].find_one({'identifier': measurement_identifier,
                                                      'start': start,
                                                      'end': end
                                                      })


            # nothing we can update if the doc doesn't exist at all, the create document
            # code branch will handle setting all the new values
            if doc == None:
                return

            existing_keys = dict((key, 1) for key in doc['values'].keys())

            cached_doc = self._cache_document_values(ptype, 
                                                     measurement_identifier,
                                                     doc['start'],
                                                     doc['end'],
                                                     point['interval'],
                                                     existing_keys)


        # examine every value type passed in  
        existing_values = cached_doc['values']                                
        new_values      = {}

        for key in values.keys():

            if not existing_values.has_key( key ):
                # create the new empty array for it
                values_array    = self._generate_data_structure_array( dimensions, size_x, size_y, size_z )
                new_values[key] = values_array

                # add it to the local cache now that we know its there
                existing_values[key]      = 1
                

        # add the empty array for this new value type in the document
        if len(new_values.keys()) > 0:

            self.logger.debug("**** Had new values %s for identifier %s of type %s ****" % (", ".join(new_values.keys()), measurement_identifier, ptype))

            self._cache_document_values(ptype,
                                        measurement_identifier,
                                        start,
                                        end,
                                        point['interval'],
                                        existing_values)

            query = {'identifier': measurement_identifier,
                     'start': start,
                     'end': end,
                     }

            to_set = {}

            data_collection = self.mongo[ptype]['data']

            for new_value in new_values.keys():
                result = data_collection.update(dict(query.items() 
                                                     + 
                                                     [('values.%s' % new_value, {'$exists': False})]
                                                     ),
                                                {'$set': {'values.%s' % new_value: new_values[new_value]}},
                                                multi=False )
                                         

    def _generate_data_structure(self, doc_info):

        values     = doc_info['values']
        dimensions = doc_info['dimensions']
        size_x     = doc_info['size_x']
        size_y     = doc_info['size_y']
        size_z     = doc_info['size_z']
        
        structure = {}

        for key in values.keys():

            # generate array for this particular value type
            array = self._generate_data_structure_array( dimensions, size_x, size_y, size_z )

            # store this values array in our structure
            structure[key] = array

        return structure

    def _generate_data_structure_array( self, dimensions, size_x, size_y, size_z ):

        array = []

        if dimensions == 1:
            for i in range(size_x):
                array.append(None)
                 
        elif dimensions == 3:
            for i in range(size_x):
                array_x = []
                for j in range(size_y):
                    array_y = []
                    for k in range(size_z):
                        array_y.append(None)
                    array_x.append(array_y)
                array.append(array_x)

        return array

def prime_factors(n):
    factors = {}
    d = 2
    while n > 1:
        while n % d == 0:
            factors[d] = factors.get(d, 0) + 1
            n /= d
        d = d + 1          
        
    uniqued = []

    count = len(factors.keys())

    if count < 3:
        for div in factors.keys():
            if factors[div] > 1:
                uniqued.append(div)
                factors[div] -= 1
                break

    for factor in factors.keys():
        uniqued.append(factor ** factors[factor])

    return uniqued

# to make command line arguments sent as function arguments when invoking this script through other python script 
def main(config,nofork,expectedrecords):

	#print " \n  Config sent : ",config
	#print " \n Nofork  flag :  ",nofork
	#print " \n expectedrecords :",expectedrecords

        if nofork:
		#print "\n calling the control class without daemon"
		if expectedrecords != 0 :
			#print "\n calling with expected records as argument";
			Control( config_file = config, expectedrecords=expectedrecords )
		else:
			#print "\n Calling with out expected records \n"
	                Control( config_file = config )
         # fork as daemon
        else:
		#print "\n calling Control calss with daemon "
                context = daemon.DaemonContext()
                with context:
                        Control( config_file = config )

if __name__ == '__main__':

    # parse command-line options
    parser = argparse.ArgumentParser( description = 'TSDS Receiver' )    

    parser.add_argument( '--config',
                         default = DEFAULT_CONFIG_FILE,
                         help = 'config file' )

    parser.add_argument( '--nofork',
                         action = 'store_true',
                         help = 'dont fork as daemon process' )
  
    args = parser.parse_args()
  
    config_file = args.config
  
    nofork = args.nofork
  
    main(config_file,nofork,0)
   
    '''
    args = parser.parse_args()
    config_file = args.config
    nofork = args.nofork
   
    # dont fork as daemon
    if nofork:
        Control( config_file = args.config )

    # fork as daemon
    else:

        context = daemon.DaemonContext()

        with context:
            Control( config_file = args.config )
    '''
