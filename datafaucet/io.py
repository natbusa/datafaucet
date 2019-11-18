from datafaucet.engines import Engine

# generic load/save,
# use for most common options and formats,
# pro: easy to use, cons: opaque format specific params
def load(path=None, provider=None, *args, format=None, **kwargs):
    return Engine().load(path, provider, *args, format=format, **kwargs)

def save(obj, path=None, provider=None, *args, format=None, mode=None, **kwargs):
    return Engine().save(obj, path, provider, *args, format=format, mode=mode, **kwargs)

def list(provider, path=None, **kwargs):
    return Engine().list(provider, path, **kwargs)

def dataframe(data=None, columns=None):
    return Engine().dataframe(data=data, columns=columns)

def range(*args):
    return Engine().range(*args)

# csv specific load/save
def load_csv(
    #resource generic params
    path=None, provider=None, *args,
    # format specific params
    sep=None,
    header=None,
    # resource related and other custom params
    **kwargs):

    return Engine().load_csv(path, provider, *args, sep=sep, header=header, **kwargs)

def save_csv(
    obj,
    #resource generic params
    path=None, provider=None, *args, mode=None,
    # format specific params
    sep=None,
    header=None,
    # resource related and other custom params
    **kwargs):

    return Engine().save_csv(obj, path, provider, mode=mode, sep=sep, header=header, **kwargs)

# json specific load/save
def load_json(
    #resource generic params
    path=None, provider=None, *args,
    # format specific params
    lines=None,
    # resource related and other custom params
    **kwargs):

    return Engine().load_json(path, provider, *args, lines=lines, **kwargs)

def save_json(
    obj,
    #resource generic params
    path=None, provider=None, *args, mode=None,
    # format specific params
    lines=None,
    # resource related and other custom params
    **kwargs):

    return Engine().save_json(obj, path, provider, mode=mode, lines=lines, **kwargs)

# json specific load/save
def load_parquet(
    #resource generic params
    path=None, provider=None, *args,
    # format specific params
    mergeSchema=None,
    # resource related and other custom params
    **kwargs):

    return Engine().load_parquet(path, provider, *args, **kwargs)

def save_parquet(
    obj,
    #resource generic params
    path=None, provider=None, *args, mode=None,
    # format specific params
    # resource related and other custom params
    **kwargs):

    return Engine().save_parquet(obj, path, provider, mode=mode, **kwargs)

# jdbc specific load/save
def load_jdbc(
    #resource generic params
    path=None, provider=None, *args,
    # format specific params
    # resource related and other custom params
    **kwargs):

    return Engine().load_jdbc(path, provider, *args, **kwargs)

def save_jdbc(
    obj,
    #resource generic params
    path=None, provider=None, *args, mode=None,
    # format specific params
    # resource related and other custom params
    **kwargs):

    return Engine().save_jdbc(obj, path, provider, mode=mode, **kwargs)
