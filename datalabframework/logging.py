import logging

# extend logging with custom level 'NOTICE'
NOTICE_LEVELV_NUM = 25 
logging.addLevelName(NOTICE_LEVELV_NUM, "NOTICE")

try:
    from kafka import KafkaProducer
except:
    KafkaProducer = None

import getpass
import datetime
import json

import os
import sys
from numbers import Number

# import a few help methods
from datalabframework import paths
from datalabframework import files
from datalabframework import metadata

from datalabframework._utils import repo_data

def func_name(level=1):
    # noinspection PyProtectedMember
    try:
        #print(','.join([sys._getframe(x).f_code.co_name for x in range(level)]))
        name = sys._getframe(level).f_code.co_name
        if name=='<module>':
            name = sys._getframe(level+1).f_code.co_name
            
        return name 
    except:
        return '?'

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
            'dlf_session': repo_data()['hash'],
            'dlf_username': getpass.getuser(),
            'dlf_filename': os.path.relpath(files.get_current_filename(), paths.rootdir()),
            'dlf_repo_name': repo_data()['name'],
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
        d.update({'dlf_func': func_name(5)})
        d.update(kwargs.get('extra', {}))
        
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


# noinspection PyUnusedLocal
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

        logr = record
        timestamp = datetime.datetime.fromtimestamp(logr.created).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        if type(logr.msg) is dict:
            msg = logr.msg
        else:
            msg = {'message': logr.msg}

        log_record = {
            'severity': logr.levelname,
            'session': logr.dlf_session,
            '@timestamp': timestamp,
            'username': logr.dlf_username,
            'filename': logr.dlf_filename,
            'func': logr.dlf_func,
            'data': msg,
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

class DlfFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """

    def filter(self, record):
        record.dlf_func = func_name()
        return True
    
def init(md=None):
    global _logger

    md = md if md else {}

    level = levels.get(md.get('loggers', {}).get('root', {}).get('severity', 'info'))
    
    # root logger
    logging.basicConfig(level=level)
    
    # dlf logger
    logger_name = md.get('loggers', {}).get('datalabframework',{}).get('name', 'dlf')
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.handlers = []
    
    p = md.get('loggers', {}).get('datalabframework',{}).get('kafka')
    if p and p['enable'] and KafkaProducer:
        level = levels.get(p.get('severity', 'info'))
        topic = p.get('topic', 'dlf')
        hosts = p.get('hosts')

        # disable logging for 'kafka.KafkaProducer', kafka.client, kafka.conn
        # to avoid infinite logging recursion on kafka
        for i in ['kafka.KafkaProducer','kafka.client', 'kafka.conn']:
            kafka_logger = logging.getLogger(i)
            kafka_logger.propagate = False
            kafka_logger.handlers = []
            
        formatterLogstash = LogstashFormatter()
        handlerKafka = KafkaLoggingHandler(topic, hosts)
        handlerKafka.setLevel(level)
        handlerKafka.setFormatter(formatterLogstash)
        logger.addHandler(handlerKafka)

    p = md.get('loggers', {}).get('datalabframework',{}).get('stream')
    if p and p['enable']:
        level = levels.get(p.get('severity', 'info'))

        # create console handler and set level to debug
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(dlf_session)s - %(dlf_repo_name)s - %(dlf_username)s - %(dlf_filename)s - %(dlf_func)s - %(message)s')
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # stream replaces higher handlers, setting propagate to false
        logger.propagate = False
    
    adapter = LoggerAdapter(logger, {})
    
    #set global _logger
    _logger = adapter

def _notice(msg, *args, **kwargs):
    logger = getLogger()
    if logger.isEnabledFor(NOTICE_LEVELV_NUM):
        logger.log(NOTICE_LEVELV_NUM, msg, *args, **kwargs) 

def notice(msg, *args, **kwargs):     
    _notice(msg, *args, **kwargs)
    
def info(msg, *args, **kwargs):
    getLogger().info(msg, *args, **kwargs)

def warning(msg, *args, **kwargs):
    getLogger().warning(msg, *args, **kwargs)

def error(msg, *args, **kwargs):
    getLogger().error(msg, *args, **kwargs)

def fatal(msg, *args, **kwargs):
    getLogger().fatal(msg, *args, **kwargs)
