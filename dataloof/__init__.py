import builtins
builtins.__DATALOOF__ = True

from ._version import version_info, __version__

from dataloof import logging
from dataloof import project
from dataloof import metadata
from dataloof import engines

from dataloof.paths import rootdir

from dataloof.resources import Resource

from dataloof.project import (
    Project
)

from dataloof.metadata import (
    Metadata
)

from dataloof.io import (
    load,
    save,
)

from dataloof.engines import (
    #factory for all engines
    context,
    engine
)

# import specific engine directly, 
# if you don't want to use the engine factory
from dataloof.spark.engine import SparkEngine
