from datafaucet.metadata import reader
from datafaucet import paths
from datafaucet._utils import Singleton

import os
from textwrap import dedent

import pytest
from testfixtures import TempDirectory

from ruamel.yaml import YAML

yaml =  YAML()
yaml.preserve_quotes = True
yaml.indent(mapping=4, sequence=4, offset=2)

@pytest.fixture()
def tempdir():
    with TempDirectory() as dir:
        original_dir = os.getcwd()
        os.chdir(dir.path)

        # clear all Singletons at the beginning of the test
        Singleton._instances = {}

        # init Paths here
        dir.write('main.ipynb', b'')

        yield dir
        os.chdir(original_dir)


# noinspection PyProtectedMember,PyProtectedMember,PyProtectedMember,PyProtectedMember,PyProtectedMember,PyProtectedMember,PyProtectedMember,PyProtectedMember
class Test_rootdir(object):

    def test_empty(self, tempdir):
        yml = ''
        tempdir.write('loader.yml', dedent(yml).encode())
        md = { 'profile':'default', 'engine': {}, 'loggers': {}, 'providers': {}, 'resources': {}, 'variables': {}}
        assert(reader.load('default')== md)

    def test_minimal(self, tempdir):
        yml = '''\
               ---
               a:
                 b: 'ohoh'
                 c: 42
                 s: 1
               '''
        tempdir.write('loader.yml', dedent(yml).encode())
        assert(reader.read(['loader.yml'])['default']['a'] == {'b': 'ohoh', 'c': 42, 's': 1})

    def test_minimal_with_resources(self, tempdir):
        yml = '''\
                ---
                a:
                    b: 'ohoh'
                    c: 42
                    s: 1
                resources:
                    hello:
                        best:resource
               '''
        tempdir.write('loader.yml', dedent(yml).encode())
        assert(reader.read([os.path.join(tempdir.path, 'loader.yml')])['default']['a'] == {'b': 'ohoh', 'c': 42, 's': 1})
        assert(reader.read([os.path.join(tempdir.path, 'loader.yml')])['default']['resources'] == {'hello': 'best:resource'})

    def test_multiple_docs(self,tempdir):
        yml = '''\
                ---
                a:
                    b: 'ohoh'
                resources:
                    hello:
                        a: 1
                ---
                profile: second
                c:
                    d: 'lalala'
                resources:
                    world:
                        b: 2
               '''
        tempdir.write('loader.yml', dedent(yml).encode())
        assert(reader.read(['loader.yml'])['default']['a'] == {'b': 'ohoh'})
        assert(reader.read(['loader.yml'])['default']['resources'] == {'hello': {'a': 1}})
        assert (reader.read(['loader.yml'])['second']['c'] == {'d': 'lalala'})
        assert (reader.read(['loader.yml'])['second']['resources'] == {'world': {'b': 2}})

    def test_multiple_files(self,tempdir):
        yml_1 = '''\
                ---
                a:
                    b: 'ohoh'
                resources:
                    hello:
                        a: 1
                ---
                profile: second
                c:
                    d: 'lalala'
               '''
        yml_2 = '''\
                ---
                resources:
                    hello:
                        aa: 1
                ---
                profile: second
                resources:
                    world:
                        b: 2
               '''

        tempdir.write('loader.yml', dedent(yml_1).encode())
        tempdir.write('abc/loader.yml', dedent(yml_2).encode())
        assert(reader.read(['loader.yml', 'abc/loader.yml'])['default']['a'] == {'b': 'ohoh'})
        assert(reader.read(['loader.yml', 'abc/loader.yml'])['default']['resources'] == {'hello': {'a': 1, 'aa': 1}})
        assert(reader.read(['loader.yml', 'abc/loader.yml'])['second']['c'] == {'d': 'lalala'})
        assert(reader.read(['loader.yml', 'abc/loader.yml'])['second']['resources'] == {'world': {'b': 2}})

# noinspection PyProtectedMember
def test_render():
    doc = '''
        ---
        profile: default
        resources:
            input:
                path: datasets/extract/{{ profile }}
                format: parquet
                provider: local-other
        '''

    ref = {
        'resources': {
            'input': {
                'format': 'parquet',
                'path': 'datasets/extract/default',
                'provider': 'local-other'}},
        'profile': 'default'}

    md = yaml.load(dedent(doc))
    res = reader.render(md)
    assert (res == ref)


# noinspection PyProtectedMember
def test_render_multipass():
    # no multipass currently

    doc = '''
        ---
        profile: test
        resources:
            oh : '{{ profile }}'
            data:
                path: datasets/extract/{{ resources.oh }}
                format: parquet-{{ resources.oh }}
                provider: local-{{ resources.oh }}
        '''
    ref = {
        'resources': {
            'data': {
                'format': 'parquet-test',
                'path': 'datasets/extract/test',
                'provider': 'local-test'},
            'oh': 'test'},
        'profile': 'test'}

    md = yaml.load(dedent(doc))
    res = reader.render(md)
    assert (res == ref)


# noinspection PyProtectedMember
def test_render_env():
    os.environ['MYENVVAR'] = '/bin/bash'

    doc = '''
        ---
        profile: default
        variables:
            ref: default.variables.a0
            a0: "{{ env('MYENVVAR') }}"
            c0: "{{ ''|env('MYENVVAR')}}"

            a1: "{{ env('MYENVVAR','default_value') }}"
            c1: "{{ 'default_value'|env('MYENVVAR')}}"

            a2: "{{ env('UNDEFINED_ENV', 'world') }}"
            c2: "{{ 'world'|env('UNDEFINED_ENV')}}"
        '''

    ref = {'profile': 'default',
           'variables': {
               'a0': '/bin/bash',
               'a1': '/bin/bash',
               'a2': 'world',
               'c0': '/bin/bash',
               'c1': '/bin/bash',
               'c2': 'world',
               'ref': 'default.variables.a0'}}

    md = yaml.load(dedent(doc))
    res = reader.render(md)
    assert (res == ref)


# noinspection PyProtectedMember
def test_render_multipass_concat():
    os.environ['MYENVVAR'] = '/bin/bash'
    doc = '''
        ---
        variables:
            a: "hello-{{ env('NOTFOUND_DEFAULT_VALUE', 'world') }}"
            b: "one-{{ env('MYENVVAR') }}"
            c: "two-{{ variables.b }}"
            d: "three-{{ variables.c }}"
            e: "{{ variables.c + '-plus-' + variables.d }}"
        '''

    ref = {'variables': {
        'a': 'hello-world',
        'b': 'one-/bin/bash',
        'c': 'two-one-/bin/bash',
        'd': 'three-two-one-/bin/bash',
        'e': 'two-one-/bin/bash-plus-three-two-one-/bin/bash'}}

    md = yaml.load(dedent(doc))
    res = reader.render(md)
    assert (res == ref)
