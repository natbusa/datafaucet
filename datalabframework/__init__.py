import builtins
builtins.__DATALABFRAMEWORK__ = True

from ._version import version_info, __version__

from datalabframework import logging
from datalabframework import project
from datalabframework import metadata
from datalabframework import engines

from datalabframework.paths import rootdir

from datalabframework.resources import Resource

#engines
from datalabframework.spark.engine import SparkEngine

from datalabframework.engines import (
    #factory for all engines
    context,
    engine
)

from datalabframework.project import (
    Project
)

from datalabframework.metadata import (
    Metadata
)

from datalabframework.io import (
    load,
    save
)