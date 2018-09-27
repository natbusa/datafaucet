import os
import sys
import glob

from .application import DatalabframeworkApp
from traitlets.config.configurable import Configurable
from traitlets import Bool, Unicode, Int, List, Dict

from cookiecutter.main import cookiecutter

class DlfInitApp(DatalabframeworkApp):

    name = Unicode(u'datalabframework-init')
    description = "Generating a data science project template"

    config_file  = Unicode(u'',help="Load this config file").tag(config=True)
    template = Unicode(u'titanic', help="Project template").tag(config=True)

    user_name = Unicode(u'natbusa', help="Project name")
    project_name = Unicode(u'', help="Project name")

    aliases = Dict(
                dict(
                    template_name='DlfInitApp.template',
                    log_level='DlfInitApp.log_level'))

    flags = Dict(dict(debug=({'DlfInitApp':{'log_level':10}}, "Set loglevel to DEBUG")))

    def initialize(self, argv=None):
        self.parse_command_line(argv)
        if self.config_file:
            self.load_config_file(self.config_file)

        if self.extra_args:
            self.project_name = self.extra_args[0]
        else:
            self.project_name = self.template

    def start(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        dir_path = os.path.abspath(os.path.join(dir_path, '../templates'))

        filename = '{}.zip'.format(os.path.join(dir_path, self.template))
        print(filename)

        # Create project from the cookiecutter-pypackage/ template
        cookiecutter(filename, extra_context={'user_name':self.user_name, 'project_name': self.project_name})

def main():
    app = DlfInitApp()
    app.initialize()
    app.start()
