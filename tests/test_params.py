from datalabframework import params, project

import os

from textwrap import dedent

import pytest
from testfixtures import TempDirectory

@pytest.fixture()
def tempdir():
    with TempDirectory() as dir:
        original_dir = os.getcwd()
        os.chdir(dir.path)

        p = project.Config()
        p.__class__._instances={}

        project.Config(dir.path)
        yield dir
        os.chdir(original_dir)


# noinspection PyProtectedMember,PyProtectedMember,PyProtectedMember,PyProtectedMember,PyProtectedMember,PyProtectedMember,PyProtectedMember,PyProtectedMember
class Test_rootpath(object):

    def test_empty(self, tempdir):
        yml = ''
        tempdir.write('metadata.yml', dedent(yml).encode())
        md = {'default':{'profile':'default', 'resources': {}, 'engines': {'spark': {'context': 'spark'}},
              'loggers': {'stream': {'enable': True, 'severity': 'info'}}, 'providers': {}, 'variables': {}}}
        assert(params._metadata()==md)

    def test_minimal(self, tempdir):
        yml = '''\
               ---
               a:
                 b: 'ohoh'
                 c: 42
                 s: 1
               '''
        tempdir.write('metadata.yml', dedent(yml).encode())
        assert(params._metadata()['default']['a']=={'b': 'ohoh', 'c': 42, 's': 1})

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
        tempdir.write('metadata.yml', dedent(yml).encode())
        assert(params._metadata()['default']['a']=={'b': 'ohoh', 'c': 42, 's': 1})
        assert(params._metadata()['default']['resources'] == { '.hello': 'best:resource'})

    def test_multiple_docs(self,tempdir):
        yml = '''\
                ---
                a:
                    b: 'ohoh'
                resources:
                    hello:
                        a:1
                ---
                profile: second
                c:
                    d: 'lalala'
                resources:
                    world:
                        b: 2
               '''
        tempdir.write('metadata.yml', dedent(yml).encode())
        assert(params._metadata()['default']['a']=={'b': 'ohoh'})
        assert(params._metadata()['default']['resources']=={'.hello': 'a:1'})
        assert (params._metadata()['second']['c'] == {'d': 'lalala'})
        assert (params._metadata()['second']['resources'] == {'.hello': 'a:1', '.world': {'b': 2}})

    def test_multiple_files(self,tempdir):
        yml_1 = '''\
                ---
                a:
                    b: 'ohoh'
                ---
                profile: second
                c:
                    d: 'lalala'
               '''
        yml_2 = '''\
                ---
                resources:
                    hello:
                        a:1
                ---
                profile: second
                resources:
                    world:
                        b: 2
               '''

        tempdir.write('metadata.yml', dedent(yml_1).encode())
        tempdir.write('abc/metadata.yml', dedent(yml_2).encode())
        assert(params._metadata()['default']['a']=={'b': 'ohoh'})
        assert(params._metadata()['default']['resources'] == {'.abc.hello': 'a:1'})
        assert(params._metadata()['second']['c']=={'d': 'lalala'})
        assert(params._metadata()['second']['resources']=={'.abc.hello': 'a:1', '.abc.world': {'b': 2}})


class Test_metadata_info(object):
    def test_multiple_files(self,tempdir):
        yml_1 = '''\
                ---
                a:
                    b: 'ohoh'
                ---
                profile: second
                c:
                    d: 'lalala'
               '''
        yml_2 = '''\
                ---
                resources:
                    hello:
                        a:1
                ---
                profile: second
                resources:
                    world:
                        b: 2
               '''

        tempdir.write('__main__.py', b'')
        tempdir.write('metadata.yml', dedent(yml_1).encode())
        tempdir.write('abc/metadata.yml', dedent(yml_2).encode())
        res = ['metadata.yml', os.path.join('abc', 'metadata.yml')]
        assert(params.metadata_files()==res)
