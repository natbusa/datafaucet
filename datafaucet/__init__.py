import builtins
builtins.__DATAFAUCET__ = True

from ._version import version_info, __version__

from datafaucet import logging
from datafaucet import project
from datafaucet import metadata
from datafaucet import engines

from datafaucet.paths import rootdir

from datafaucet.resources import Resource

from datafaucet.project import (
    Project
)

from datafaucet.metadata import (
    Metadata
)

from datafaucet.io import (
    load,
    save,
    list,
    range
)

# import engine factory method
from datafaucet.engines import engine
# import specific engine directly
from datafaucet.spark.engine import SparkEngine
from datafaucet.pandas.engine import PandasEngine
from datafaucet.dask.engine import DaskEngine

from datafaucet import web
from datafaucet import crypto
