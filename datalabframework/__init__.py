import sys

from IPython.core.interactiveshell import InteractiveShell
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)

#automatically imports submodules
from . import export
from . import log
from . import notebook
from . import params
from . import project

__all__ = ['export', 'log', 'notebook', 'params', 'project']

@magics_class
class DataLabFramework(Magics):

    @cell_magic
    def datalabframework(self, line, cell):
        "A lean data science framework"
        # keep the magic to the minimum
        # add the detected ipython filename in the framework
        # the magic overwrite the env variable
        return project.detect_filename()

def load_ipython_extension(ipython):
    # The `ipython` argument is the currently active `InteractiveShell`
    # instance, which can be used in any way. This allows you to register
    # new magics or aliases, for example.
    
    def _run_cell_magic(self, magic_name, line, cell):
        if len(cell) == 0 or cell.isspace():
            fn = self.find_line_magic(magic_name)
            if fn:
                return _orig_run_line_magic(self, magic_name, line)
            # IPython will complain if cell is empty string 
            # but not if it is None
            cell = None
        return _orig_run_cell_magic(self, magic_name, line, cell)


    _orig_run_cell_magic = InteractiveShell.run_cell_magic
    InteractiveShell.run_cell_magic = _run_cell_magic

    # In order to actually use these magics, you must register them with a
    # running IPython.  This code must be placed in a file that is loaded once
    # IPython is up and running:
    ip = get_ipython()
    # You can register the class itself without instantiating it.  IPython will
    # call the default constructor on it.
    ip.register_magics(DataLabFramework)


def unload_ipython_extension(ipython):
    # If you want your extension to be unloadable, put that logic here.
    pass

# Add rootpath() if available
if project.rootpath() and project.rootpath() not in sys.path: 
    sys.path.append(project.rootpath())

# register hook for loading ipynb files
sys.meta_path.append(project.NotebookFinder())