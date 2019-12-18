from datafaucet import logging

# keep only one object out of a set of classes
# register to use the factory

_singleton = {'instance': None, 'args': (), 'kwargs': {}}


class EngineSingleton(type):
    def __call__(cls, *args, **kwargs):
        global _singleton

        if not _singleton['instance']:
            _singleton['instance'] = super(EngineSingleton, cls).__call__(*args, **kwargs)
            _singleton['args'] = args
            _singleton['kwargs'] = kwargs
            logging.debug('Engine created', cls.__name__)
            return _singleton['instance']

        # a different Engine or Engine Configuration?
        # stop the previous, re-instantiate the engine

        diff_engine = _singleton['instance'].__class__.__name__ != cls.__name__
        diff_args = _singleton['args'] != args
        diff_kwargs = _singleton['kwargs'] != kwargs

        if diff_engine or diff_args or diff_kwargs:
            logging.debug(f"Factory: Stop the current {_singleton['instance'].__class__.__name__} instance")

            _singleton['instance'].stop()
            del _singleton['instance']

            # print(f"Factory: Start new {cls.__name__} instance")
            _singleton['instance'] = super(EngineSingleton, cls).__call__(*args, **kwargs)
            _singleton['args'] = args
            _singleton['kwargs'] = kwargs
            return _singleton['instance']
        else:
            # print('SAME: returing the current singleton')
            pass

        return _singleton['instance']


_engines = {}


def register(cls, alias):
    global _engines

    _engines[cls.__name__] = cls
    _engines[alias] = cls

    logging.debug('Registering names ', cls.__name__, alias, ' for class ', cls)


def Engine(engine_type=None, *args, **kwargs):
    global _engines

    if engine_type:
        if engine_type in _engines.keys():
            cls = _engines[engine_type]
            cls(*args, **kwargs)
        else:
            logging.error('Could not create the Engine:')
            logging.error('No matching engine type in', ', '.join(_engines.keys()))

    eng = _singleton['instance']

    if not eng:
        logging.error(
            'No Engine running yet. \n'
            'try datafaucet.engine(...) or datafaucet.project.load(...)')

    return eng


def engine(engine_type=None, *args, **kwargs):
    return Engine(engine_type, *args, **kwargs)


class EngineBase:
    def __init__(self, engine_type=None, session_name=None, session_id=None):
        self.engine_type = engine_type
        self.session_name = session_name
        self.session_id = session_id

        # print statement
        logging.debug(f'Init engine "{self.engine_type}"')

        self.submit = dict()
        self.info = dict()
        self.conf = dict()
        self.context = None
        self.version = None
        self.env = dict()
        self.stopped = True

    def load(self, *args, **kwargs):
        raise NotImplementedError

    def save(self, obj, path=None, provider=None, *args, mode='error', **kwargs):
        raise NotImplementedError

    def copy(self, *args, **kwargs):
        raise NotImplementedError

    def list(self, provider, path):
        raise NotImplementedError

    def range(self, provider, path):
        raise NotImplementedError

    def stop(self):
        self.stopped = True
