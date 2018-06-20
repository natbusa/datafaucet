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

#import a few help methods
from . import project
from . import notebook
from . import params

_logger = None

def _default_json_default(obj):
    """
    Coerce everything to strings.
    All objects representing time get output as ISO8601.
    """
    if  isinstance(obj, datetime.datetime) or \
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

        :param datefmt: Date format to use (required by logging.Formatter interface but not used)
        :param json_cls: JSON encoder to forward to json.dumps
        :param json_default: Default JSON representation for unknown types, by default coerce everything to a string
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

    def format(self, record):
        """
        Format a log record to JSON, if the message is a dict
        assume an empty message and use the dict as additional
        fields.
        """

        print(record)
        d = record.__dict__.copy()
        loginfo = {k:d.get(k,None) for k in ['created', 'levelname', 'exc_info']}

        loginfo['exception'] = None
        if loginfo['exc_info']:
            formatted = tb.format_exception(*loginfo['exc_info'])
            loginfo['exception'] = formatted
            loginfo.pop('exc_info')

        info = self.defaults.copy()
        info.update({'log_level': loginfo['levelname'],'log_exception': loginfo['exception']})

        fields = dict()
        message = None
        if isinstance(record.msg, dict):
            fields = record.msg
        else:
            message = record.getMessage()


        timestamp = datetime.datetime.fromtimestamp(loginfo['created']).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        logr = {'message': message,
                'info': info,
                '@timestamp': timestamp,
                'fields': fields}

        return json.dumps(logr, default=self.json_default, cls=self.json_cls)

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
    'warnning': logging.WARNING,
    'error': logging.ERROR,
    'fatal': logging.FATAL
}

def init():
    global _logger

    md = params.metadata()

    info = dict()
    info.update({'username': getpass.getuser()})
    info.update({'filename': notebook.filename(ext='')[1], 'filepath': notebook.filename()[0]})

    logger = logging.getLogger()
    logger.handlers = []

    level = loggingLevels.get(md['logging'].get('severity'))
    logger.setLevel(level)

    p = md['logging']['handlers'].get('kafka')
    if p and p['enable'] and KafkaProducer:

        level = loggingLevels.get(p.get('severity'))
        topic = p.get('topic')
        hosts = p.get('hosts')

        #disable logging for 'kafka.KafkaProducer'
        logging.getLogger('kafka.KafkaProducer').addHandler(logging.NullHandler())

        formatterLogstash = LogstashFormatter(json.dumps({'extra':info}))
        handlerKafka = KafkaLoggingHandler(topic, hosts)
        handlerKafka.setLevel(level)
        handlerKafka.setFormatter(formatterLogstash)
        logger.addHandler(handlerKafka)


    p = md['logging']['handlers'].get('stream')
    if p and p['enable']:
        level = loggingLevels.get(p.get('severity'))

        # create console handler and set level to debug
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - {} - {} - {} - {} - {} - {} - {} - {} - {} - {} - %(message)s'.format(*info.values()))
        handler = logging.StreamHandler(sys.stdout,)
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    _logger = logger

def logger():
    global _logger

    if not _logger:
        init()

    return _logger
