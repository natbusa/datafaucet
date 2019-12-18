import logging
import datetime
import json

import sys
from numbers import Number
from collections import MutableMapping
from datafaucet.utils import merge

import inspect
import ntpath

import getpass
import uuid

from datafaucet import paths
from datafaucet import files
from datafaucet import git

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None

# extend logging with custom level 'NOTICE'
NOTICE = 25
logging.addLevelName(NOTICE, "NOTICE")


def get_scope(level=1):
    try:
        frame = inspect.stack(context=0)[level]
        # the scope is the file in the library where the log is produced
        scope = ntpath.basename(frame.filename).split(".")[0]
        func = frame.function

        # for interactive logging, prettify the scope/func ...
        if scope.startswith('<ipython'):
            scope = 'notebook'
            func = 'cell' if func == '<module>' else func

        return f'{scope}:{func}'
    except:
        return '-:-'


# logging object is a singleton
_logger = logging.getLogger('datafaucet')


def getLogger():
    global _logger
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
        d.update({'dfc_funcname': get_scope(5)})

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


def init_adapter(logger=None, sid=None):
    sid = sid or hex(uuid.uuid1().int >> 64)
    username = getpass.getuser()
    filepath = files.get_script_path(paths.rootdir())

    repo = git.repo_data()
    reponame = repo['name']
    repohash = repo['hash']

    # configure context
    extra = {
        'dfc_sid': sid,
        'dfc_repohash': repohash,
        'dfc_reponame': reponame,
        'dfc_username': username,
        'dfc_filepath': filepath
    }

    # setup adapter
    return LoggerAdapter(logger, extra)


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

    def __init__(self, fmt=None, datefmt=None):
        super().__init__(fmt=fmt, datefmt=datefmt)

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
        self.producer = None
        try:
            self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        except Exception as e:
            print('WARNING:datafaucet:KafkaLoggingHandler', str(e), ' - disabling kafka logging handler')

    def emit(self, record):
        if self.producer:
            try:
                msg = self.format(record).encode("utf-8")
                self.producer.send(self.topic, msg)
            except Exception as e:
                print('WARNING:datafaucet:KafkaLoggingHandler', str(e), ' - skip kafka logging statement')

    def flush(self):
        if self.producer:
            try:
                self.producer.flush()
            except Exception as e:
                print('WARNING:datafaucet:KafkaLoggingHandler', str(e), ' - could not flush')
        logging.Handler.flush(self)

    def close(self):
        if self.producer:
            try:
                if hasattr(KafkaProducer, 'stop'):
                    self.producer.stop()
                self.producer.close()
            except Exception as e:
                print('WARNING:datafaucet:KafkaLoggingHandler', str(e), ' - could not stop')

        logging.Handler.close(self)


levels = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'notice': NOTICE,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}

kafka_handler = None


def init_kafka(logger, level, hosts=None):
    global kafka_handler

    try:
        if kafka_handler:
            kafka_handler.flush()
            kafka_handler.close()
    except Exception as e:
        print(e)

    if not hosts:
        return

    topic = 'dfc'

    # # disable logging for 'kafka.KafkaProducer', kafka.conn
    # for i in ['kafka.KafkaProducer', 'kafka.conn']:
    #     kafka_logger = logging.getLogger(i)
    #     kafka_logger.propagate = False
    #     kafka_logger.handlers = []

    formatter = JsonFormatter()
    kafka_handler = KafkaLoggingHandler(topic, hosts)
    kafka_handler.setLevel(level)
    kafka_handler.setFormatter(formatter)
    logger.addHandler(kafka_handler)


def init_stdout(logger, level, enable=True):
    if not enable:
        return

    # '%(asctime)s',
    # '%(levelname)s',
    # '%(dfc_sid)s',
    # '%(dfc_repohash)s',
    # '%(dfc_reponame)s',
    # '%(dfc_filepath)s',
    # '%(dfc_funcname)s'
    # '%(message)s'
    # '%(dfc_data)s')

    # create console handler and set level to debug
    formatter = logging.Formatter(' [%(name)s] %(levelname)s %(dfc_filepath)s:%(dfc_funcname)s | %(message)s')

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


file_handler = None


def init_file(logger, level, filename=None):
    global file_handler

    if not filename:
        return

    try:
        if file_handler:
            file_handler.flush()
            file_handler.close()
    except Exception as e:
        print(e)

    try:
        formatter = JsonFormatter()
        file_handler = logging.FileHandler(filename)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        print(e)
        print(f'Cannot initialize log file {filename}')


def init(
        log_level=logging.INFO,
        log_stdout=None,
        log_filename=None,
        log_kafka_hosts=None,
        sid=None,
        propagate=False):
    global _logger

    logger = logging.getLogger('datafaucet')

    # setting log level (str or integer)
    log_level = levels.get(log_level.lower()) if isinstance(log_level, str) else log_level
    logger.setLevel(log_level)

    # init handlers
    logger.handlers = []
    init_stdout(logger, log_level, log_stdout)
    init_file(logger, log_level, log_filename)
    init_kafka(logger, log_level, log_kafka_hosts)

    # stream replaces higher handlers, setting propagate to false
    logger.propagate = propagate

    # set global _logger
    _logger = init_adapter(logger, sid)


def _notice(msg, extra=None, **kwargs):
    logger = getLogger()
    if logger.isEnabledFor(NOTICE):
        logger.log(NOTICE, msg, extra=extra, **kwargs)


def debug(*args, extra=None, **kwargs):
    msg = ' '.join(map(str, args))
    getLogger().debug(msg, extra=extra, **kwargs)


def info(*args, extra=None, **kwargs):
    msg = ' '.join(map(str, args))
    getLogger().info(msg, extra=extra, **kwargs)


def notice(*args, extra=None, **kwargs):
    msg = ' '.join(map(str, args))
    _notice(msg, extra=extra, **kwargs)


def warning(*args, extra=None, **kwargs):
    msg = ' '.join(map(str, args))
    getLogger().warning(msg, extra=extra, **kwargs)


def error(*args, extra=None, **kwargs):
    msg = ' '.join(map(str, args))
    getLogger().error(msg, extra=extra, **kwargs)


def critical(*args, extra=None, **kwargs):
    msg = ' '.join(map(str, args))
    getLogger().critical(msg, extra=extra, **kwargs)
