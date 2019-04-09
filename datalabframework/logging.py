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


# import a few help methods
from datalabframework import paths
from datalabframework import files

from datalabframework._utils import repo_data, merge

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
            'dlf_sid': None,
            'dlf_username': None,
            'dlf_filepath': None,
            'dlf_reponame': None,
            'dlf_repohash': None,
            'dlf_funcname': None,
            'dlf_data': {}
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
        d.update({'dlf_funcname': func_name(5)})
        
        if isinstance(msg, dict):
            d.update({'dlf_data': merge(msg, kwargs.get('extra', {}))})
            msg = 'data'
        elif isinstance(msg, str):
            d.update({'dlf_data': kwargs.get('extra', {})})
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
            'sid': logr.dlf_sid,
            'repohash': logr.dlf_repohash,
            'reponame': logr.dlf_reponame,
            'username': logr.dlf_username,
            'filepath': logr.dlf_filepath,
            'funcname': logr.dlf_funcname,
            'message': logr.msg,
            'data': logr.dlf_data
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
    'fatal': logging.FATAL
}

def init_kafka(logger, level, md):
    p = md.get('datalabframework',{}).get('kafka')
    if p and p.get('enable', True) and KafkaProducer:
        level = levels.get(p.get('severity', level))
        topic = p.get('topic', 'dlf')
        hosts = p.get('hosts')
    else:
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

def init_stdio(logger, level, md):    
    p = md.get('datalabframework',{})
    p = p.get('stream') or p.get('stdio')
    if p and p.get('enable', True):
        level = levels.get(p.get('severity', level))
    else:
        return
    
    # create console handler and set level to debug
    formatter = logging.Formatter(' - '.join([
        #'%(asctime)s',
        '%(levelname)s',
        #'%(dlf_sid)s',
        #'%(dlf_repohash)s',
        #'%(dlf_reponame)s',
        #'%(dlf_filepath)s',
        '%(dlf_funcname)s',
        '%(message)s',
        '%(dlf_data)s'
        ]))
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

file_handler = None
def init_file(logger, level, md):    
    global file_handler

    p = md.get('datalabframework',{}).get('file')
    if p and p.get('enable', True):
        level = levels.get(p.get('severity', level))
    else:
        return

    path = p.get('path', f'{logger.name}.log')
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

    md = md if md else {}

    # root logger
    level = levels.get(md.get('root', {}).get('severity', 'info'))
    logging.basicConfig(level=level)
    
    # dlf logger
    logger_name = md.get('datalabframework',{}).get('name', 'dlf')
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.handlers = []
    
    # init handlers
    init_kafka(logger, level, md)
    init_stdio(logger, level, md)
    init_file(logger, level, md)
    
    # stream replaces higher handlers, setting propagate to false
    logger.propagate = False
    
    # configure context
    dlf_extra = {
        'dlf_sid': sid,
        'dlf_repohash': repohash,
        'dlf_reponame': reponame,
        'dlf_username': username,
        'dlf_filepath': filepath
    }
    
    # setup adapter
    adapter = LoggerAdapter(logger, dlf_extra)
    
    #set global _logger
    _logger = adapter

def _notice(msg, *args, **kwargs):
    logger = getLogger()
    if logger.isEnabledFor(NOTICE_LEVELV_NUM):
        logger.log(NOTICE_LEVELV_NUM, msg, *args, **kwargs) 

def info(msg, *args, **kwargs):
    getLogger().info(msg, *args, **kwargs)

def notice(msg, *args, **kwargs):     
    _notice(msg, *args, **kwargs)

def warning(msg, *args, **kwargs):
    getLogger().warning(msg, *args, **kwargs)

def error(msg, *args, **kwargs):
    getLogger().error(msg, *args, **kwargs)

def fatal(msg, *args, **kwargs):
    getLogger().fatal(msg, *args, **kwargs)
