import logging

# extend logging with custom level 'NOTICE'
NOTICE_LEVELV_NUM = 25 
logging.addLevelName(NOTICE_LEVELV_NUM, "NOTICE")

try:
    from kafka import KafkaProducer
except:
    KafkaProducer = None

import datetime
import json

import os
import sys
from numbers import Number
from collections import MutableMapping

# import a few help methods
from datafaucet import paths
from datafaucet import files

from datafaucet._utils import repo_data, merge

def func_name(level=1):
    try:
        name = sys._getframe(level).f_code.co_name
        if name=='<module>':
            name = sys._getframe(level+1).f_code.co_name
            
        return name 
    except:
        return '-'

# logging object is a singleton
_logger = None

def getLogger():
    global _logger
    if not _logger:
        init()
    return _logger

class LoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger, extra):
        """
        Initialize the adapter with a logger and a dict-like object which
        provides contextual information. This constructor signature allows
        easy stacking of LoggerAdapters, if so desired.
        You can effectively pass keyword arguments as shown in the
        following example:
        adapter = LoggerAdapter(someLogger, dict(p1=v1, p2="v2"))
        """
        self.logger = logger
        self.extra = {
            'dfc_sid': None,
            'dfc_username': None,
            'dfc_filepath': None,
            'dfc_reponame': None,
            'dfc_repohash': None,
            'dfc_funcname': None,
            'dfc_data': {}
        }
        
        self.extra.update(extra)

    def process(self, msg, kwargs):
        """
        Process the logging message and keyword arguments passed in to
        a logging call to insert contextual information. You can either
        manipulate the message itself, the keyword args or both. Return
        the message and kwargs modified (or not) to suit your needs.
        Normally, you'll only need to override this one method in a
        LoggerAdapter subclass for your specific needs.
        """
        d = self.extra
        d.update({'dfc_funcname': func_name(5)})
        
        if isinstance(msg, MutableMapping):
            merged = merge(msg, kwargs.get('extra', {}))
            d.update({'dfc_data': merged})
            msg = 'data'
        elif isinstance(msg, str):
            d.update({'dfc_data': kwargs.get('extra', {})})
        else:
            raise ValueError('log message must be a str or a dict')
            
        kwargs["extra"] = d
        return msg, kwargs


def _json_default(obj):
    """
    Coerce everything to strings.
    All objects representing time get output as ISO8601.
    """
    if isinstance(obj, datetime.datetime) or \
            isinstance(obj, datetime.date) or \
            isinstance(obj, datetime.time):
        return obj.isoformat()
    elif isinstance(obj, Number):
        return obj
    else:
        return str(obj)


class JsonFormatter(logging.Formatter):
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

        logr = record
        timestamp = datetime.datetime.fromtimestamp(logr.created)
        timestamp = timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')

        log_record = {
            '@timestamp': timestamp,
            'severity': logr.levelname,
            'sid': logr.dfc_sid,
            'repohash': logr.dfc_repohash,
            'reponame': logr.dfc_reponame,
            'username': logr.dfc_username,
            'filepath': logr.dfc_filepath,
            'funcname': logr.dfc_funcname,
            'message': logr.msg,
            'data': logr.dfc_data
        }

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
            if hasattr(KafkaProducer, 'stop'):
                self.producer.stop()
            self.producer.close()
        logging.Handler.close(self)


levels = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'notice': NOTICE_LEVELV_NUM,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}

def init_kafka(logger, level, md):
    p = md['datafaucet']['kafka'] 
    if p and p['enable'] and KafkaProducer:
        level = levels.get(p['severity'] or level)
        topic = p['topic'] or 'dfc'
        hosts = p['hosts']
    else:
        return
    
    if not hosts:
        logging.warning('Logging on kafka: no hosts defined')
        return

    # disable logging for 'kafka.KafkaProducer', kafka.conn
    for i in ['kafka.KafkaProducer','kafka.conn']:
        kafka_logger = logging.getLogger(i)
        kafka_logger.propagate = False
        kafka_logger.handlers = []
            
    formatter = JsonFormatter()
    handlerKafka = KafkaLoggingHandler(topic, hosts)
    handlerKafka.setLevel(level)
    handlerKafka.setFormatter(formatter)
    logger.addHandler(handlerKafka)

def init_stdout(logger, level, md):    
    p = md['datafaucet']['stdout']
    
    # legacy param
    p = p or md['datafaucet']['stream']
    
    if p and p['enable']:
        level = levels.get(p['severity'] or level)
    else:
        return
    
    #'%(asctime)s',
    #'%(levelname)s',
    #'%(dfc_sid)s',
    #'%(dfc_repohash)s',
    #'%(dfc_reponame)s',
    #'%(dfc_filepath)s',
    #'%(dfc_funcname)s'
    #'%(message)s'
    #'%(dfc_data)s')
    
    # create console handler and set level to debug
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(dfc_funcname)s %(message)s')

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

file_handler = None
def init_file(logger, level, md):    
    global file_handler

    p = md['datafaucet']['file']
    if p and p['enable']:
        level = levels.get(p['severity'] or level)
    else:
        return

    path = p['path'] or f'{logger.name}.log'
    try:
        if file_handler:
            file_handler.close()
            
        file_handler = open(path, 'w')
        formatter = JsonFormatter()
        handler = logging.StreamHandler(file_handler)
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    except e:
        print(e)
        print(f'Cannot open log file {path} for writing.')
        
def init(
    md=None, 
    sid=None, 
    username=None, 
    filepath=None, 
    reponame=None, 
    repohash=None):
    
    global _logger

    if not md:
        _logger = logging.getLogger('dfc')
        return

    # root logger
    level = levels.get(md['root']['severity'] or 'info')
    logging.basicConfig(level=level)
    
    # dfc logger
    logger_name = md['datafaucet']['name'] or 'dfc'
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.handlers = []
    
    # init handlers
    init_kafka(logger, level, md)
    init_stdout(logger, level, md)
    init_file(logger, level, md)
    
    # stream replaces higher handlers, setting propagate to false
    logger.propagate = False
    
    # configure context
    dfc_extra = {
        'dfc_sid': sid,
        'dfc_repohash': repohash,
        'dfc_reponame': reponame,
        'dfc_username': username,
        'dfc_filepath': filepath
    }
    
    # setup adapter
    adapter = LoggerAdapter(logger, dfc_extra)
    
    #set global _logger
    _logger = adapter

def _notice(msg, *args, **kwargs):
    logger = getLogger()
    if logger.isEnabledFor(NOTICE_LEVELV_NUM):
        logger.log(NOTICE_LEVELV_NUM, msg, *args, **kwargs) 

def debug(msg, *args, **kwargs):
    getLogger().debug(msg, *args, **kwargs)

def info(msg, *args, **kwargs):
    getLogger().info(msg, *args, **kwargs)

def notice(msg, *args, **kwargs):     
    _notice(msg, *args, **kwargs)

def warning(msg, *args, **kwargs):
    getLogger().warning(msg, *args, **kwargs)

def error(msg, *args, **kwargs):
    getLogger().error(msg, *args, **kwargs)

def critical(msg, *args, **kwargs):
    getLogger().critical(msg, *args, **kwargs)
