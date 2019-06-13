from datalabframework.engines import Engine

def load(*args, **kwargs):
    return Engine().load(*args, **kwargs)

def save(obj, *args, **kwargs):
    return Engine().save(obj, *args, **kwargs)
