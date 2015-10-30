#!/usr/bin/python

import pymongo
import time
import pika
import json
import httplib
import logging
import logging.config
import xml.etree.ElementTree as xml

xml_tree = xml.parse( '/etc/grnoc/tsds/services/config.xml' )
config   = xml_tree.getroot()

INTERVAL           = 300
SERVER_STATUS_TYPE = 'meta_tsds_server'
SHARD_STATUS_TYPE  = 'meta_tsds_shard'
DB_STATUS_TYPE     = 'meta_tsds_db'
RABBIT_STATUS_TYPE = 'meta_tsds_rabbit'
QUEUE              = config.find( 'rabbit' ).attrib.get( 'queue' )
RABBIT_HOST        = config.find( 'rabbit' ).attrib.get( 'host' )
RABBIT_PORT        = int( config.find( 'rabbit' ).attrib.get( 'port' ) )
RABBIT_MGMT_PORT   = int( config.find( 'rabbit' ).attrib.get( 'mgmt_port' ) or 15672)

logging.config.fileConfig('/etc/grnoc/tsds/services/meta_logging.conf')
logger = logging.getLogger('metatsds')

logger.debug("Rabbit connection info => queue \"%s\" on %s:%s" % (QUEUE, RABBIT_HOST, RABBIT_PORT))

def pullOut(d, name):
    out = {}
    for k in d[name]:
        if isinstance(d[name][k], (int, long, float)):
            out[name+"_"+k] = d[name][k]
    return out

def now():
    return int(time.time())

def makeDoc(type):
    return {'time': now(), 'interval': INTERVAL, 'type': type, 'meta': {}, 'values': {}} 

def serverStatus(cli, shard = None):
    logger.debug("serverStatus")
    types = ['asserts', 'opcounters', 'connections']
    if shard:
        doc = makeDoc(SHARD_STATUS_TYPE)
        types.append('cursors')
        doc['meta']['shard'] = shard
    else:
        doc = makeDoc(SERVER_STATUS_TYPE)

    try:
        status = cli.admin.command('serverStatus')
    except pymongo.errors.ConnectionFailure:
        logger.critical("Unable to get server stats for %s." % repr(cli))
        return []

    doc['meta']['host'] = status['host']

    for k in types:
        doc['values'].update(pullOut(status, k))

    del(doc['values']['connections_totalCreated']) ## Can't handle the diff yet

    return [doc]



def shardStatus(cli):
    logger.debug("shardStatus")
    try:
        cursor = cli.config.shards.find()
    except pymongo.errors.ConnectionFailure:
        logger.critical("Unable to find shards from %s" %repr(cli))
        return []

    docs = []

    for shard in cursor:
        try:
            shardCli = pymongo.MongoClient(shard['host'])
        except pymongo.errors.ConnectionFailure:
            logger.error("Unable to connect to shard %s" % shard['host'])
            continue
        doc = serverStatus(shardCli, shard = shard['_id'])
        docs.extend(doc)
        docs.extend(dbStatus(shardCli, shard['_id']))
    return docs

def dbStatus(cli, shard = None):
    logger.debug("dbStatus")
    docs = []
    try:
        dbs = cli.database_names()
    except pymongo.errors.ConnectionFailure:
        logger.critical("Unable to get database names from %s." % repr(cli))
        return []
    for db in dbs:
        if db.startswith("_"):
            logger.debug("Skipping %s because it starts with an underscore." % db)
            continue
        doc = makeDoc(DB_STATUS_TYPE)
        try:
            stats = cli[db].eval('db.stats()')
        except pymongo.errors.ConnectionFailure:
            logger.error("Unable to get stats for %s %s. Skipping." % (repr(cli), db))
            continue
        if('dataFileVersion' in stats): del stats['dataFileVersion']
        if('ok' in stats):              del stats['ok']
        if('db' in stats):              del stats['db']

        doc['values'] = stats
        doc['meta']['db'] = db
        if shard:
            doc['meta']['shard'] = shard
        docs.append(doc)
    return docs

def rabbitStatus():
    logger.debug("rabbitStatus")
    doc = makeDoc(RABBIT_STATUS_TYPE)

    logger.debug("rabbitStatus -- get")
    try:
        ws=httplib.HTTP("{0}:{1}".format(RABBIT_HOST, RABBIT_MGMT_PORT))
        ws.putrequest("GET", '/api/nodes')
        ws.putheader("Authorization", "Basic %s" % "guest:guest".encode('base64'))
        ws.endheaders()
        ws.send("")
        statuscode, statusmessage, header = ws.getreply()
        raw = ws.getfile().read()
    except:
        logger.error("Unable to get Rabbit node stats")
        return []
    logger.debug("rabbitStatus -- load")
    try:
        nodes = json.loads(raw)
    except:
        logger.error("Unable to parse Rabbit node stats JSON.")
        return []
    doc['meta']['name'] = nodes[0]['name']
    for k in ['fd_used', 'fd_total', 'sockets_used', 'sockets_total', 'mem_used', 'mem_limit', 'disk_free_limit', 'disk_free', 'proc_used', 'proc_total', 'run_queue']:
        doc['values'][k] = nodes[0][k]
    logger.debug("rabbitStatus -- get overview")
    try:
        ws.putrequest("GET", '/api/overview?lengths_age=%s&lengths_incr=5&msg_rates_age=%s&msg_rates_incr=5'%(INTERVAL,INTERVAL))
        ws.putheader("Authorization", "Basic %s" % "guest:guest".encode('base64'))
        ws.endheaders()
        ws.send("")
        statuscode, statusmessage, header = ws.getreply()
        raw = ws.getfile().read()
    except:
        logger.error("Unable to get Rabbit overview stats")
        return []
    logger.debug("rabbitStatus -- load overview")
    try:
        overview = json.loads(raw)
    except:
        logger.error("Unable to parse Rabbit overview JSON.")
        return []
    logger.debug("a")
    #for k in ['publish', 'ack', 'deliver', 'redeliver', 'get_no_ack']:
    for k in ['publish', 'ack', 'deliver', 'redeliver']:
        logger.debug(k)
        doc['values']['message_'+k] = overview['message_stats'][k+"_details"]['rate']
        logger.debug("done")
    logger.debug("b")
    for k in ['messages', 'messages_ready', 'messages_unacknowledged']:
        doc['values']['queue_'+k] = overview['queue_totals'][k+"_details"]['rate']
    logger.debug("c")
    doc['values'].update(pullOut(overview, 'object_totals'))
    logger.debug("rabbitStatus -- done")
    return [doc]

def send(docs):
    logger.debug("send")
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host = RABBIT_HOST,
                                           port = RABBIT_PORT))
        channel = connection.channel()

        channel.queue_declare(queue   = QUEUE, 
                      durable = False)

        channel.basic_publish(exchange='',
                      routing_key = QUEUE,
                      body        = json.dumps(docs))

    except Exception, e:
        logger.critical("Unable to send doc to Rabbit! %s" % e)

def main():
    logger.debug("main")
  
    cli = None
    try:
        cli = pymongo.MongoClient()
    except pymongo.errors.ConnectionFailure as e:
        logger.critical("Unable to connect to mongodb: {0}".format(e))
        return

    docs = []

    docs += shardStatus(cli)
    docs += rabbitStatus()

    send(docs)

    logger.debug(json.dumps(docs, sort_keys=True, indent=4, separators=(',', ': ')))

if __name__ == "__main__":
    main()
