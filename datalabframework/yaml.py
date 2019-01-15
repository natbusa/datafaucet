from ruamel.yaml import YAML
from ruamel.yaml.compat import StringIO

class StringDumpYAML(YAML):
    def dump(self, data, stream=None, **kw):
        inefficient = False
        if stream is None:
            inefficient = True
            stream = StringIO()
        YAML.dump(self, data, stream, **kw)
        if inefficient:
            return stream.getvalue()


yaml = StringDumpYAML()
yaml.preserve_quotes = True
yaml.indent(mapping=4, sequence=4, offset=2)