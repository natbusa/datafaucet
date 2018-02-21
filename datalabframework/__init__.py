import sys

#automatically imports submodules
from . import export
from . import log
from . import notebook
from . import project
from . import params
from . import data
from . import engines

__all__ = ['export', 'log', 'notebook', 'project', 'params', 'data', 'engines']

# Add rootpath() if available
if project.rootpath() and project.rootpath() not in sys.path: 
    sys.path.append(project.rootpath())

# register hook for loading ipynb files
sys.meta_path.append(project.NotebookFinder())