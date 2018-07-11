import os
import sys

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
    running = Bool(False,
                   help="Is the app running?").tag(config=True)

    classes = List([Bar, Foo, ExecutePreprocessor])

    config_file = Unicode(u'',
                   help="Load this config file").tag(config=True)

    aliases = Dict(dict(timeout='Foo.timeout', i='Foo.i',j='Foo.j',name='Foo.name', running='DlfRunApp.running',
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

    def initialize(self, argv=None):
        self.parse_command_line(argv)
        if self.config_file:
            self.load_config_file(self.config_file)
        self.init_foo()
        self.init_bar()
        self.init_preprocessor()

    def start(self):
        print("app.config:")
        print(self.config)
        #sys.argv[1]
        filename = '/home/natbusa/Projects/datalabframework/simple.ipynb'

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

def main():
    app = DlfRunApp()
    app.initialize()
    app.start()
