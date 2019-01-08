import os
import getpass

from .application import DatalabframeworkApp
from traitlets import Unicode, Dict

from cookiecutter.main import cookiecutter


class DlfInitApp(DatalabframeworkApp):
    name = Unicode(u'datalabframework-init')
    description = "Generating a data science project template"

    config_file = Unicode(u'', help="Load this config file").tag(config=True)

    template = Unicode(u'default', help="Project template").tag(config=True)
    username = Unicode(getpass.getuser(), help="Author name").tag(config=True)
    name = Unicode(u'datalab-project', help="Project name").tag(config=True)

    aliases = Dict({
        'template': 'DlfInitApp.template',
        'username': 'DlfInitApp.username',
        'name': 'DlfInitApp.name'
    })

    flags = Dict()

    def initialize(self, argv=None):
        self.parse_command_line(argv)
        if self.config_file:
            self.load_config_file(self.config_file)

        if self.extra_args:
            self.name = self.extra_args[0]

    def start(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        dir_path = os.path.abspath(os.path.join(dir_path, 'templates'))

        filename = '{}'.format(os.path.join(dir_path, self.template))

        # Create project from the cookiecutter-pypackage/ template
        cookiecutter(filename, extra_context={'username': self.username, 'project_name': self.name})


def main():
    app = DlfInitApp()
    app.initialize()
    app.start()
