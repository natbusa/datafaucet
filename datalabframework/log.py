import logging
from kafka import KafkaProducer

import socket
import datetime
import traceback as tb
import json

import getpass
import sys
import os

#import a few help methods
from . import project

def _default_json_default(obj):
    """
    Coerce everything to strings.
    All objects representing time get output as ISO8601.
    """
    if isinstance(obj, datetime.datetime) or \
       isinstance(obj,datetime.date) or      \
       isinstance(obj,datetime.time):
        return obj.isoformat()
    else:
        return str(obj)

class LogstashFormatter(logging.Formatter):
    """
    A custom formatter to prepare logs to be
    shipped out to logstash.
    """

    def __init__(self,
                 fmt=None,
                 datefmt=None,
                 json_cls=None,
                 json_default=_default_json_default):
        """
        :param fmt: Config as a JSON string, allowed fields;
               extra: provide extra fields always present in logs
               source_host: override source host name
        :param datefmt: Date format to use (required by logging.Formatter
            interface but not used)
        :param json_cls: JSON encoder to forward to json.dumps
        :param json_default: Default JSON representation for unknown types,
                             by default coerce everything to a string
        """

        if fmt is not None:
            self._fmt = json.loads(fmt)
        else:
            self._fmt = {}
        self.json_default = json_default
        self.json_cls = json_cls
        if 'extra' not in self._fmt:
            self.defaults = {}
        else:
            self.defaults = self._fmt['extra']
        if 'source_host' in self._fmt:
            self.source_host = self._fmt['source_host']
        else:
            try:
                self.source_host = socket.gethostname()
            except:
                self.source_host = ""

    def format(self, record):
        """
        Format a log record to JSON, if the message is a dict
        assume an empty message and use the dict as additional
        fields.
        """

        fields = record.__dict__.copy()

        if isinstance(record.msg, dict):
            fields.update(record.msg)
            fields.pop('msg')
            msg = ""
        else:
            msg = record.getMessage()

        if 'msg' in fields:
            fields.pop('msg')

        if 'exc_info' in fields:
            if fields['exc_info']:
                formatted = tb.format_exception(*fields['exc_info'])
                fields['exception'] = formatted
            fields.pop('exc_info')

        if 'exc_text' in fields and not fields['exc_text']:
            fields.pop('exc_text')

        logr = self.defaults.copy()

        logr.update({'@message': msg,
                     '@timestamp': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                     '@source_host': self.source_host,
                     '@fields': self._build_fields(logr, fields)})

        return json.dumps(logr, default=self.json_default, cls=self.json_cls)

    def _build_fields(self, defaults, fields):
        d = defaults.get('@fields', {})
        d.update(fields)
        d.update({'username':getpass.getuser()})

        return d

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

def initLogger(name, level = logging.DEBUG, kafka_topic=None, kafka_servers=None):
    
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # create kafka handler and set level to debug
    if kafka_topic and kafka_servers:

        #disable logging for 'kafka.KafkaProducer'
        logging.getLogger('kafka.KafkaProducer').addHandler(logging.NullHandler())

        formatterLogstash = LogstashFormatter()
        handlerKafka = KafkaLoggingHandler(kafka_topic, kafka_servers)
        handlerKafka.setLevel(level)
        handlerKafka.setFormatter(formatterLogstash)
        logger.addHandler(handlerKafka)
        
    # create console handler and set level to debug
    formatter = logging.Formatter('%(asctime)s - {} - %(name)s - %(levelname)s - %(message)s - %(context)s'.format(getpass.getuser()))
    handler = logging.StreamHandler(sys.stdout,)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    #first log entry here
    extra= {'context': 
        {
            'notebook': {
                'filename':project.filename(), 
                'filepath':os.path.dirname(os.path.abspath(project.filename())) if project.filename() else None
            },
            'project': {
                'main': 'main.ipynb',
                'rootpath':project.rootpath(), 
            },
            'datalab': {
                'framework': '0.1'
            }
        }
    }

    logger.info('init', extra=extra)

    return logger
