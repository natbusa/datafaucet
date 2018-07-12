import os
import sys
import glob

from textwrap import dedent

from .application import DatalabframeworkApp

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

from traitlets.config.configurable import Configurable

from traitlets import (
    Bool, Unicode, Int, List, Dict
)

class Foo(Configurable):
    """A class that has configurable, typed attributes.
    """

    timeout = Int(0, help="The timeout.").tag(config=True)
    i = Int(0, help="The integer i.").tag(config=True)
    j = Int(1, help="The integer j.").tag(config=True)
    name = Unicode(u'Brian', help="First name.").tag(config=True)


class Bar(Configurable):

    enabled = Bool(True, help="Enable bar.").tag(config=True)


class DlfRunApp(DatalabframeworkApp):

    name = Unicode(u'datalabframework-run')
    description = "Executing a datalabframework notebook"


    running = Bool(False,
                   help="Is the app running?").tag(config=True)

    classes = List([Bar, Foo, ExecutePreprocessor])

    config_file = Unicode(u'',
                   help="Load this config file").tag(config=True)

    notebooks = List([], help="""List of notebooks to convert.
                     Wildcards are supported.
                     Filenames passed positionally will be added to the list.
                     """
    ).tag(config=True)

    aliases = Dict(dict(timeout='ExecutePreprocessor.timeout', i='Foo.i',j='Foo.j',name='Foo.name', running='DlfRunApp.running',
                        enabled='Bar.enabled', log_level='DlfRunApp.log_level'))

    flags = Dict(dict(enable=({'Bar': {'enabled' : True}}, "Enable Bar"),
                  disable=({'Bar': {'enabled' : False}}, "Disable Bar"),
                  debug=({'MyApp':{'log_level':10}}, "Set loglevel to DEBUG")
            ))

    def init_foo(self):
        # Pass config to other classes for them to inherit the config.
        self.foo = Foo(config=self.config)

    def init_bar(self):
        # Pass config to other classes for them to inherit the config.
        self.bar = Bar(config=self.config)

    def init_preprocessor(self):
        self.ep = ExecutePreprocessor(config=self.config)

    def init_notebooks(self):
        if self.extra_args:
            patterns = self.extra_args
        else:
            patterns = self.notebooks

        # Use glob to replace all the notebook patterns with filenames.
        filenames = []
        for pattern in patterns:

            # Use glob to find matching filenames.  Allow the user to convert
            # notebooks without having to type the extension.
            globbed_files = glob.glob(pattern)
            globbed_files.extend(glob.glob(pattern + '.ipynb'))
            if not globbed_files:
                self.log.warning("pattern %r matched no files", pattern)

            for filename in globbed_files:
                if not filename in filenames:
                    filenames.append(filename)
        self.notebooks = filenames


    def initialize(self, argv=None):
        self.parse_command_line(argv)
        if self.config_file:
            self.load_config_file(self.config_file)
        self.init_notebooks()
        self.init_foo()
        self.init_bar()
        self.init_preprocessor()

    def run_single_notebook(self,filename):
        nb = nbformat.read(filename, as_version=4)
        filename_tuple =  os.path.split(filename)

        fullpath_filename = os.path.join(os.getcwd(), *filename_tuple)
        cwd = os.path.dirname(fullpath_filename)
        init_str = dedent("""
            # added by dlf-run
            import datalabframework as dlf
            dlf.project.Init('{}', '{}')
            """.format(cwd,fullpath_filename))

        print(init_str)

        nc = nbformat.v4.new_code_cell(init_str)
        nb['cells'].insert(0, nc)

        resources ={}
        resources['metadata'] = {'path': os.getcwd()}

        (nb_out, resources_out) = self.ep.preprocess(nb, resources)
        print(nb_out)

    def start(self):
        print("app.config:")
        print(self.config)

        print(self.notebooks)

        for notebook_filename in self.notebooks:
            self.run_single_notebook(notebook_filename)

def main():
    app = DlfRunApp()
    app.initialize()
    app.start()
