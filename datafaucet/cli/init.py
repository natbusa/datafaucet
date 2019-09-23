import os
import getpass

from .application import DatafaucetApp
from traitlets import Unicode, Dict, Bool

from cookiecutter.main import cookiecutter


class DfcInitApp(DatafaucetApp):
    name = Unicode(u'datafaucet-init')
    description = "Generating a BI/ML project template"

    config_file = Unicode(u'', help="Load this config file").tag(config=True)

    template = Unicode(u'default', help="Project template").tag(config=True)
    username = Unicode(getpass.getuser(), help="Author username").tag(config=True)
    fullname = Unicode(getpass.getuser(), help="Author full name").tag(config=True)
    name = Unicode(u'dfc-project', help="Project name").tag(config=True)
    desc = Unicode(u'Project scaffolding for ETL and Data Science', help="Project name").tag(config=True)

    prompt = Bool(False, help=u'Suppress cli input via prompt').tag(config=True)

    aliases = Dict({
        'template': 'DfcInitApp.template',
        'username': 'DfcInitApp.username',
        'fullname': 'DfcInitApp.fullname',
        'name': 'DfcInitApp.name',
        'desc': 'DfcInitApp.desc',
        'prompt': 'DfcInitApp.prompt'
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

        # content from cli or prompt (if enabled)
        extra_context={
            'username': self.username, 
            'fullname': self.fullname, 
            'project_name': self.name, 
            'project_short_description': self.desc
        }
        
        # Create project from the cookiecutter-pypackage/ template
        cookiecutter(filename, no_input=self.prompt, extra_context=extra_context)


def main():
    app = DfcInitApp()
    app.initialize()
    app.start()
