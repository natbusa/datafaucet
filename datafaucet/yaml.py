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
yaml.width = 4096
yaml.preserve_quotes = True
yaml.indent(mapping=4, sequence=4, offset=2)

# loaded yaml objects implement dict and list and seq.
# The following change the representation to yaml text

from ruamel.yaml.comments import CommentedMap, CommentedSeq, CommentedSet

yaml_repr = lambda self: yaml.dump(self)

# replace representation with yaml dump,
# according to the flow/indent defined above

CommentedSeq.__repr__ = yaml_repr
CommentedMap.__repr__ = yaml_repr
CommentedSet.__repr__ = yaml_repr


# yaml dict (todo: remove, use to_yaml)
def YamlDict(*args, **kargs):
    if len(args) > 0 and isinstance(args[0], str):
        d = yaml.load(args[0])

        # check if the top object is a dictionary
        if not isinstance(d, dict):
            raise ValueError(
                'the input yaml string does not describe a valid dictionary')
    else:
        # from python objects to CommentedMap via a yaml dump
        d = yaml.load(yaml.dump(dict(*args, **kargs)))

    return d


# generator
def to_yaml(obj):
    if isinstance(obj, str):
        d = yaml.load(obj)
    else:
        # from python objects to Commented classes via a yaml dump
        d = yaml.load(yaml.dump(obj))
    return d


# to std python types (hierarchical traverse, deep first)
def to_python_type(obj):
    if isinstance(obj, dict):
        return {k: to_python_type(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_python_type(v) for v in obj]
    elif isinstance(obj, tuple):
        return (to_python_type(v) for v in obj)
    elif isinstance(obj, set):
        return {to_python_type(v) for v in obj}
    else:
        return obj


def to_dict(obj):
    if not isinstance(obj, dict):
        raise ValueError('the input is not a valid dict')
    return to_python_type(obj)


def to_set(obj):
    if not isinstance(obj, set):
        raise ValueError('the input is not a valid set')
    return to_python_type(obj)


def to_list(obj):
    if not isinstance(obj, list):
        raise ValueError('the input is not a valid list')
    return to_python_type(obj)


# add the corresponding hierarchical casting to the "Commented" Classes
CommentedSeq.to_list = lambda self: to_python_type(self)
CommentedMap.to_dict = lambda self: to_python_type(self)
CommentedSet.to_set = lambda self: to_python_type(self)
