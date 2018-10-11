import logging

try:
    from kafka import KafkaProducer
except:
    KafkaProducer=None

import socket
import getpass
import datetime
import traceback as tb
import json

import sys
import os
from numbers import Number

#import a few help methods
from . import project
from . import notebook
from . import params

_logger = None

def _json_default(obj):
    """
    Coerce everything to strings.
    All objects representing time get output as ISO8601.
    """
    if  isinstance(obj, datetime.datetime) or \
        isinstance(obj,datetime.date) or      \
        isinstance(obj,datetime.time):
        return obj.isoformat()
    elif isinstance(obj, Number):
        return obj
    else:
        return str(obj)

def custom_attributes(record):
    if type(record.msg) is dict and 'type' in record.msg:
        record.type = record.msg['type']
        del record.msg['type']
    else:
        record.type = 'message'

    # add all the magic
    record.session = project.repository()['hash']
    # add all the magic
    record.username = getpass.getuser()
    record.filename = project.filename()
    return record

class StreamFormatter(logging.Formatter):
    def __init__(self,
                 fmt=None,
                 datefmt=None):
        super().__init__(fmt, datefmt)

    def format(self, record):
        return super(StreamFormatter, self).format(custom_attributes(record))

class LogstashFormatter(logging.Formatter):
    """
    A custom formatter to prepare logs to be
    shipped out to logstash.
    """

    def __init__(self,
                 fmt=None,
                 datefmt=None):
        pass

    def format(self, record):
        """
        Format a log record to JSON, if the message is a dict
        assume an empty message and use the dict as additional
        fields.
        """

        logr =  custom_attributes(record)
        timestamp = datetime.datetime.fromtimestamp(loginfo['created']).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        # loginfo = {k:d.get(k,None) for k in ['created', 'levelname', 'exc_info']}
        # loginfo['exception'] = None
        # if loginfo['exc_info']:
        #     formatted = tb.format_exception(*loginfo['exc_info'])
        #     loginfo['exception'] = formatted
        #     loginfo.pop('exc_info')

        log_record = {
            'severity': logr.levelname,
            'session': session,
            '@timestamp': timestamp,
            'type': type,
            'fields': fields}

        return json.dumps(log_record, default=_json_default)

class KafkaLoggingHandler(logging.Handler):

    def __init__(self, topic, bootstrap_servers):
        logging.Handler.__init__(self)

        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def emit(self, record):
        msg = self.format(record).encode("utf-8")
        self.producer.send(self.topic, msg)

    def close(self):
        if self.producer is not None:
            self.producer.flush()
            self.producer.stop()
        logging.Handler.close(self)

loggingLevels = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'fatal': logging.FATAL
}

def init():
    global _logger

    md = params.metadata()

    info = dict()

    logger = logging.getLogger()
    level = loggingLevels.get(md['loggers'].get('severity', 'info'))
    logger.setLevel(level)
    logger.handlers = []

    p = md['loggers'].get('kafka')
    if p and p['enable'] and KafkaProducer:

        level = loggingLevels.get(p.get('severity'))
        topic = p.get('topic')
        hosts = p.get('hosts')

        # disable logging for 'kafka.KafkaProducer'
        # to avoid infinite logging recursion on kafka
        logging.getLogger('kafka.KafkaProducer').addHandler(logging.NullHandler())

        formatterLogstash = LogstashFormatter(json.dumps({'extra':info}))
        handlerKafka = KafkaLoggingHandler(topic, hosts)
        handlerKafka.setLevel(level)
        handlerKafka.setFormatter(formatterLogstash)
        logger.addHandler(handlerKafka)


    p = md['loggers'].get('stream')
    if p and p['enable']:
        level = loggingLevels.get(p.get('severity'))

        # create console handler and set level to debug
        formatter = StreamFormatter('%(asctime)s - %(levelname)s - %(session)s - %(username)s - %(filename)s - %(type)s - %(message)s')
        handler = logging.StreamHandler(sys.stdout,)
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    _logger = logger

def getLogger():
    global _logger
    if not _logger:
        init()
    return _logger
