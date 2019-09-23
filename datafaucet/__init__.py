import builtins
builtins.__DATALOOF__ = True

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
)

from datafaucet.engines import (
    #factory for all engines
    context,
    engine
)

# import specific engine directly, 
# if you don't want to use the engine factory
from datafaucet.spark.engine import SparkEngine
