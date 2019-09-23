from datafaucet.metadata import resource
from datafaucet._utils import merge
from datafaucet import paths

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

        
# resource('SELECT 0 as result where 1 = 0', 'pagila')
# resource('foo.csv', '/bar')
# resource('foo.csv', 'bar')
# resource('foo.csv', 'hdfs')
# resource('/foo.abc', 'hdfs')
# resource('/foo.abc', 'test')
# resource('hello/foo.abc', 'test')
# resource('foo.abc', 'hdfs://hdfs-namenode:8020/wanna/dance/with/somebody')
# resource('/foo.abc', 'hdfs://hdfs-namenode/wanna/dance/with/somebody')
# resource('/foo.abc', 'hdfs://hdfs-namenode:8020/wanna/dance/with/somebody')
# resource('staff', 'jdbc:mysql://1.2.3.4:3306/sakila?useSSL=false&serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL')
# resource('staff', 'jdbc:mysql://pippo:baudo@1.2.3.4:3306/sakila?useSSL=false&serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL')
# resource('staff', 'jdbc:mysql://1.2.3.4/sakila', useSSL='false', serverTimezone='UTC', zeroDateTimeBehavior='CONVERT_TO_NULL')
# resource('staff', service='mysql', database='sakila', serverTimezone='UTC')
# resource('sakila/staff', service='mysql', serverTimezone='UTC', user='pippo', password='baudo')
# resource('foo/bar.tsv', service='s3a')
# resource('/foo/bar.tsv', service='s3a')
# resource('/apples/orange', service='minio')
# resource('SELECT count(*) as cnt from employees;', 'jdbc:mysql://1.2.3.4:3306/sakila?useSSL=false&serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL', user='pippo', password='baudo')
# resource('ascombe')
# resource('ascombe', 'saywhat')
# resource('ascombe', 'hdfs://hdfs-namenode:8020/otherpath/')
# resource('ascombe', 'hdfs')
# resource('ascombe', 'test')
# resource('r_test', 'test')

# urn = 'jdbc:oracle:thin:name/pass@//123.123.123:345/schema/database'
# parsed = Urn(scheme=['jdbc', 'oracle', 'thin', 'name/pass@'], user='name', password='pass', host='123.123.123', port='345', path='/schema/database', params=[], query='', fragment='')

# assert(parsed == urnparse(urn))

# urn = 'jdbc:oracle:thin:name@//123.123.123:345/schema/database'
# parsed = Urn(scheme=['jdbc', 'oracle', 'thin', 'name@'], user='name', password='', host='123.123.123', port='345', path='/schema/database', params=[], query='', fragment='')

# assert(parsed == urnparse(urn))

# urn = 'jdbc:oracle:thin:@//123.123.123/schema/database'
# parsed = Urn(scheme=['jdbc', 'oracle', 'thin', '@'], user='', password='', host='123.123.123', port='', path='/schema/database', params=[], query='', fragment='')

# assert(parsed == urnparse(urn))

# urn = 'hdfs://123.123.123/schema/database'
# parsed = Urn(scheme=['hdfs'], user='', password='', host='123.123.123', port='', path='/schema/database', params=[], query='', fragment='')

# assert(parsed == urnparse(urn))

# urn = '/schema/database'
# parsed = Urn(scheme=[], user='', password='', host='', port='', path='/schema/database', params=[], query='', fragment='')

# assert(parsed == urnparse(urn))

# urn = 's3a://schema/database'
# parsed = Urn(scheme=['s3a'], user='', password='', host='', port='', path='schema/database', params=[], query='', fragment='')

# assert(parsed == urnparse(urn))

# urn = '1.2.34/schema/database'
# parsed = Urn(scheme=[], user='', password='', host='', port='', path='1.2.34/schema/database', params=[], query='', fragment='')

# assert(parsed == urnparse(urn))

# urn = 'file://1.2.34/schema/database'
# parsed = Urn(scheme=['file'], user='', password='', host='1.2.34', port='', path='/schema/database', params=[], query='', fragment='')

# assert(parsed == urnparse(urn))

# urn = 'jdbc:sqlite://localdir/a/b/c'
# parsed = Urn(scheme=['jdbc', 'sqlite'], user='', password='', host='', port='', path='localdir/a/b/c', params=[], query='', fragment='')

# assert(parsed == urnparse(urn))

# urn = 'jdbc:oracle:thin:@ldap://xyz.acme.com:7777/sales,cn=salesdept,cn=OracleContext,dc=com'
# parsed = Urn(scheme=['jdbc', 'oracle', 'thin', '@ldap'], user='', password='', host='xyz.acme.com', port='7777', path='/sales', params=[('cn', 'salesdept'), ('cn', 'OracleContext'), ('dc', 'com')], query='cn=salesdept&cn=OracleContext&dc=com', fragment='')

# assert(parsed == urnparse(urn))

# urn = 'http://xyz.acme.com:7777/foo/bar?a=1&edf=abc#anchor1'
# parsed = Urn(scheme=['http'], user='', password='', host='xyz.acme.com', port='7777', path='/foo/bar', params=[('a', '1'), ('edf', 'abc')], query='a=1&edf=abc', fragment='anchor1')

# assert(parsed == urnparse(urn))

# urn = 'jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks;integratedSecurity=true;'
# parsed = Urn(scheme=['jdbc', 'sqlserver'], user='', password='', host='localhost', port='1433', path='', params=[('databaseName', 'AdventureWorks'), ('integratedSecurity', 'true')], query='databaseName=AdventureWorks&integratedSecurity=true', fragment='')

# assert(parsed == urnparse(urn))

# urn = 'jdbc:sqlserver://localhost;databaseName=AdventureWorks;integratedSecurity=true;'
# parsed = Urn(scheme=['jdbc', 'sqlserver'], user='', password='', host='localhost', port='', path='', params=[('databaseName', 'AdventureWorks'), ('integratedSecurity', 'true')], query='databaseName=AdventureWorks&integratedSecurity=true', fragment='')

# assert(parsed == urnparse(urn))

# urn = 'jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=false'
# parsed = Urn(scheme=['jdbc', 'postgresql'], user='', password='', host='localhost', port='', path='/test', params=[('user', 'fred'), ('password', 'secret'), ('ssl', 'false')], query='user=fred&password=secret&ssl=false', fragment='')

# assert(parsed == urnparse(urn))

# urn = 'jdbc:mysql://localhost:3306/youdatabase?useSSL=false&serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL'
# parsed = Urn(scheme=['jdbc', 'mysql'], user='', password='', host='localhost', port='3306', path='/youdatabase', params=[('useSSL', 'false'), ('serverTimezone', 'UTC'), ('zeroDateTimeBehavior', 'CONVERT_TO_NULL')], query='useSSL=false&serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL', fragment='')

# assert(parsed == urnparse(urn))