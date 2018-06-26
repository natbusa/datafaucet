from datalabframework import params

import os
from textwrap import dedent

import pytest
from testfixtures import TempDirectory


@pytest.fixture()
def dir():
    with TempDirectory() as dir:
        os.chdir(dir.path)
        yield dir

class Test_rootpath(object):
    def test_minimal(self, dir):
        yml = '''\
               ---
               a:
                 b: 'ohoh'
                 c: 42
                 s: 1
               '''
        dir.write('metadata.yml', dedent(yml).encode())
        assert(params.metadata()=={'a': {'b': 'ohoh', 'c': 42, 's': 1}, 'resources': {}})

    def test_minimal_with_resources(self, dir):
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
        dir.write('metadata.yml', dedent(yml).encode())
        assert(params.metadata()=={'a': {'b': 'ohoh', 'c': 42, 's': 1}, 'resources': { '.hello': 'best:resource'}})

    def test_multiple_docs(self,dir):
        yml = '''\
                ---
                a:
                    b: 'ohoh'
                resources:
                    hello:
                        a:1
                ---
                run: second
                c:
                    d: 'lalala'
                resources:
                    world:
                        b: 2
               '''
        dir.write('metadata.yml', dedent(yml).encode())
        assert(params.metadata()=={'a': {'b': 'ohoh'}, 'resources': {'.hello': 'a:1'}})
        assert(params.metadata(True)=={
            'default': {'a': {'b': 'ohoh'}, 'resources': {'.hello': 'a:1'}},
            'second': {'c': {'d': 'lalala'},'resources': {'.world': {'b': 2}},'run': 'second'}
            })

    def test_multiple_files(self,dir):
        yml_1 = '''\
                ---
                a:
                    b: 'ohoh'
                ---
                run: second
                c:
                    d: 'lalala'
               '''
        yml_2 = '''\
                ---
                resources:
                    hello:
                        a:1
                ---
                run: second
                resources:
                    world:
                        b: 2
               '''

        subdir = dir.makedir('abc')
        dir.write('metadata.yml', dedent(yml_1).encode())
        dir.write('abc/metadata.yml', dedent(yml_2).encode())
        assert(params.metadata()=={'a': {'b': 'ohoh'}, 'resources': {'.abc.hello': 'a:1'}})
        assert(params.metadata(True)=={
            'default': {'a': {'b': 'ohoh'}, 'resources': {'.abc.hello': 'a:1'}},
            'second': {'c': {'d': 'lalala'},'resources': {'.abc.world': {'b': 2}},'run': 'second'}
            })

class Test_metadata_info(object):
    def test_multiple_files(self,dir):
        yml_1 = '''\
                ---
                a:
                    b: 'ohoh'
                ---
                run: second
                c:
                    d: 'lalala'
               '''
        yml_2 = '''\
                ---
                resources:
                    hello:
                        a:1
                ---
                run: second
                resources:
                    world:
                        b: 2
               '''

        subdir = dir.makedir('abc')
        dir.write('__main__.py', b'')
        dir.write('metadata.yml', dedent(yml_1).encode())
        dir.write('abc/metadata.yml', dedent(yml_2).encode())
        res = {
            'files': ['metadata.yml', 'abc/metadata.yml'],
            'rootpath': dir.path,
            'runs': ['default', 'second']
            }
        assert(params.metadata_info()==res)
