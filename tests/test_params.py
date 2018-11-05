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

class Test_rootpath(object):
    def test_minimal(self, tempdir):
        yml = '''\
               ---
               a:
                 b: 'ohoh'
                 c: 42
                 s: 1
               '''
        tempdir.write('metadata.yml', dedent(yml).encode())
        assert(params._metadata()['default']=={'a': {'b': 'ohoh', 'c': 42, 's': 1}, 'resources': {}, 'engines':{}, 'loggers':{}, 'providers': {}, 'variables': {}})

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
        assert(params._metadata()['default']=={'a': {'b': 'ohoh', 'c': 42, 's': 1}, 'resources': { '.hello': 'best:resource'},'engines':{}, 'loggers':{}, 'providers': {},  'variables': {}})

    def test_minimal_with_rendering(self, tempdir):
        yml = '''\
                ---
                a:
                    b: 'ohoh'
                    c: 42
                    s: ping-{{ default.foo.bar.best }}
                foo:
                    bar:
                        best: pong
               '''
        tempdir.write('metadata.yml', dedent(yml).encode())
        assert(params._metadata()['default']=={'a': {'b': 'ohoh', 'c': 42, 's': 'ping-pong'}, 'foo': { 'bar': {'best':'pong'}}, 'resources': {}, 'engines':{}, 'loggers':{}, 'providers': {},  'variables': {}})

    def test_minimal_with_rendering_multiple_docs(self, tempdir):
        yml = '''\
                ---
                a:
                    b: 'ohoh'
                    c: 42
                    s: ping-{{ ping.foo.bar.best }}
                ---
                profile: ping
                foo:
                    bar:
                        best: pong

               '''
        tempdir.write('metadata.yml', dedent(yml).encode())
        assert(params._metadata()['default']=={'a': {'b': 'ohoh', 'c': 42, 's': 'ping-pong'}, 'resources': {}, 'engines':{}, 'loggers':{}, 'providers': {},  'variables': {}})

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
        assert(params._metadata()['default']=={'a': {'b': 'ohoh'}, 'resources': {'.hello': 'a:1'},'engines':{}, 'loggers':{}, 'providers': {}, 'variables': {}})
        assert(params._metadata()['second']=={'c': {'d': 'lalala'},'resources': {'.hello': 'a:1', '.world': {'b': 2}},'engines':{}, 'loggers':{}, 'providers': {},'profile': 'second', 'variables': {}})

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
        assert(params._metadata()['default']=={'a': {'b': 'ohoh'}, 'resources': {'.abc.hello': 'a:1'}, 'engines':{}, 'loggers':{}, 'providers': {}, 'variables': {}})
        assert(params._metadata()['second']=={'c': {'d': 'lalala'},'resources': {'.abc.hello': 'a:1', '.abc.world': {'b': 2}},'engines':{}, 'loggers':{}, 'providers': {},'profile': 'second', 'variables': {}})

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
