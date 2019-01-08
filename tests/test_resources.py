from datalabframework.metadata import resource
from datalabframework._utils import merge
from datalabframework import paths

import os

import pytest
from testfixtures import TempDirectory

@pytest.fixture()
def tempdir():
    with TempDirectory() as d:
        original_dir = os.getcwd()
        os.chdir(d.path)

        d.write('main.ipynb', b'')

        yield d
        os.chdir(original_dir)


def test_port():
    # noinspection PyProtectedMember
    resource._port('postgres')
    # noinspection PyProtectedMember
    assert (resource._port('postgres') == 5432)

def test_format():
    # noinspection PyProtectedMember
    assert (resource._format({'service': 'postgres'}, {}) == 'jdbc')
    # noinspection PyProtectedMember
    assert (resource._format({'service': 'elastic'}, {}) == 'nosql')
    # noinspection PyProtectedMember
    assert (resource._format({'service': 'hdfs'}, {'format': 'csv'}) == 'csv')
    # noinspection PyProtectedMember
    assert (resource._format({'service': 'hdfs'}, {}) == 'parquet')
    # noinspection PyProtectedMember
    assert (resource._format({}) == None)

def test_driver():
    # noinspection PyProtectedMember
    assert (resource._driver({'service': 'postgres'}) == 'org.postgresql.Driver')
    # noinspection PyProtectedMember
    assert (resource._format({'service': 'notdefined'}) == None)
    # noinspection PyProtectedMember
    assert (resource._format({}) == None)

class Test_get_resource_metadata(object):
    def test_empty(self):

        metadata = {
            'providers': {},
            'resources': {}
        }
        # noinspection PyProtectedMember
        d = resource._get_resource_metadata(metadata=metadata)
        assert(d == {})

    def test_incomplete(self):

        metadata = {
            'providers': {
                'a': {}
            },
            'resources': {}
        }
        # noinspection PyProtectedMember
        d = resource._get_resource_metadata(provider='a', metadata=metadata)
        assert(d == {'provider':'a'})

    def test_minimal(self):

        metadata = {
            'providers': {
                'p': {}
            },
            'resources': { 'r': { 'provider': 'p'}}
        }
        # noinspection PyProtectedMember
        d = resource._get_resource_metadata(resource='r', provider='p', metadata=metadata)
        assert(d == {'alias': 'r', 'provider': 'p'})

        # noinspection PyProtectedMember
        d = resource._get_resource_metadata(resource='r', provider='p_not', metadata=metadata)
        assert(d == {'alias': 'r', 'provider': 'p_not'})

    def test_match_with_resource_name(self):

        metadata = {
            'providers': {
                'p': {}
            },
            'resources': {
                'r1.r2.r3': { 'path': 'abc', 'provider': 'p'},
                'r2': { 'path': 'abc', 'provider': 'p'},

            }
        }
        # noinspection PyProtectedMember
        d = resource._get_resource_metadata(resource='r1.r2.r3', provider='p', metadata=metadata)
        assert(d == {'alias': 'r1.r2.r3', 'path': 'abc', 'provider': 'p'})

        # noinspection PyProtectedMember
        d = resource._get_resource_metadata(resource='r1.r2.r3', provider='p_not', metadata=metadata)
        assert(d == {'alias': 'r1.r2.r3', 'path': 'abc', 'provider': 'p_not'})

        # noinspection PyProtectedMember
        d = resource._get_resource_metadata(resource='other', provider='p_not', metadata=metadata)
        assert(d == {'path': 'other', 'provider': 'p_not'})

    def test_match_with_path(self):

        metadata = {
            'providers': {
                'p': {}
            },
            'resources': {
                'r1': { 'path': 'abc', 'provider': 'p'},
                'r2': { 'path': 'abc', 'provider': 'p'},

            }
        }
        # noinspection PyProtectedMember
        d = resource._get_resource_metadata(resource='abc', provider='p', metadata=metadata)
        assert(d == {'alias': 'r1', 'path': 'abc', 'provider': 'p'})
        # noinspection PyProtectedMember
        d = resource._get_resource_metadata(resource='abc', provider='p_not', metadata=metadata)
        assert(d == {'alias': 'r1', 'path': 'abc', 'provider': 'p_not'})

    def test_no_match(self):

        metadata = {
            'providers': {
                'p': {}
            },
            'resources': {
                'r1': { 'path': 'abc', 'provider': 'p'},
                'r2': { 'path': 'abc', 'provider': 'p'},

            }
        }
        # noinspection PyProtectedMember
        d = resource._get_resource_metadata(resource='xyz', provider='abc', metadata=metadata)
        assert(d == {'path': 'xyz', 'provider': 'abc'})

class Test_build_resource_metadata(object):
    def empty(self, tempdir):
        return {
            'database': None,
            'driver': None,
            'format': 'parquet',
            'hostname': '127.0.0.1',
            'port': None,
            'provider_alias': None,
            'password': None,
            'username': None,
            'provider_path': tempdir.path,
            'read': {'cache':False, 'filter': {}, 'mapping': {}, 'options': {}, 'partition': {}},
            'write': {'cache':False, 'filter': {}, 'mapping': {}, 'options': {}, 'partition': {}},
            'resource_alias': None,
            'resource_path': '',
            'service': 'file',
            'url': tempdir.path,
        }

    def test_test(self,tempdir):
        assert tempdir.path

    def test_empty(self, tempdir):
        # noinspection PyProtectedMember
        d = resource._build_resource_metadata(tempdir.path)
        assert(d == self.empty(tempdir))

    def test_minimal(self, tempdir):
        pmd = {
            'service': 'local',
            'format': 'csv',
            'path': tempdir.path
        }
        # noinspection PyProtectedMember
        d = resource._build_resource_metadata(tempdir.path, pmd=pmd)

        m = self.empty(tempdir).copy()
        u = {
            'provider_path': tempdir.path,
            'service': 'local',
            'format': 'csv',
            'url': f'{tempdir.path}'
        }
        m = merge(m,u)

        assert(d == m)

    def test_resource_provider(self, tempdir):
        pmd = {
            'alias': 'p',
            'service': 'local',
            'format': 'csv',
            'path': tempdir.path
        }
        rmd = {
            'alias': 'r',
            'path': 'abc/def'
        }
        # noinspection PyProtectedMember
        d = resource._build_resource_metadata(tempdir.path, pmd=pmd, rmd=rmd)

        m = self.empty(tempdir).copy()
        u = {
            'provider_path': tempdir.path,
            'provider_alias': 'p',
            'resource_alias': 'r',
            'resource_path': 'abc/def',
            'service': 'local',
            'format': 'csv',
            'url': f'{tempdir.path}/abc/def'
        }
        m = merge(m,u)


        assert(d == m)

    def test_resource_provider_2path_absolute(self, tempdir):
        pmd = {
            'alias': 'p',
            'service': 'local',
            'format': 'csv',
            'path': '/absolute/path'
        }
        rmd = {
            'alias': 'r',
            'path': 'abc/def'
        }
        # noinspection PyProtectedMember
        d = resource._build_resource_metadata(tempdir.path, pmd=pmd, rmd=rmd)

        m = self.empty(tempdir).copy()
        u = {
            'provider_path': '/absolute/path',
            'provider_alias': 'p',
            'resource_alias': 'r',
            'resource_path': 'abc/def',
            'service': 'local',
            'format': 'csv',
            'url': f'/absolute/path/abc/def'
        }
        m = merge(m,u)

        assert(d == m)
