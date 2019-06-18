import builtins
builtins.__DATALABFRAMEWORK__ = True

from ._version import version_info, __version__

from datalabframework import logging
from datalabframework import project
from datalabframework import metadata
from datalabframework import engines

from datalabframework.paths import rootdir

from datalabframework.resources import Resource

from datalabframework.project import (
    Project
)

from datalabframework.metadata import (
    Metadata
)

from datalabframework.io import (
    load,
    load_csv,
    load_json,
    load_parquet,
    load_jdbc,

    save,
    save_csv,
    save_json,
    save_parquet,
    save_jdbc,
)

from datalabframework.engines import (
    #factory for all engines
    context,
    engine
)

#engines
from datalabframework.spark.engine import SparkEngine
