from datafaucet import files, paths

import os
from textwrap import dedent

import pytest
from testfixtures import TempDirectory

from datafaucet._utils import Singleton

@pytest.fixture()
def tempdir():
    with TempDirectory() as dir:
        original_dir = os.getcwd()
        os.chdir(dir.path)

        # clear all Singletons at the beginning of the test
        Singleton._instances = {}

        dir.write('main.ipynb', b'')

        yield dir
        os.chdir(original_dir)

def os_sep(path):
    return path.replace("/", os.sep)

def test_get_project_files(tempdir):
        tempdir.makedir('abc')
        tempdir.makedir('abc/def')
        tempdir.makedir('excluded')
        tempdir.makedir('excluded/xyz')
        tempdir.makedir('ignored')
        tempdir.makedir('ignored/xyz')
        tempdir.makedir('123')
        tempdir.makedir('123/xyz')
        tempdir.makedir('123/ignored')
        tempdir.write('md.yml', b'')
        tempdir.write('1.txt', b'')
        tempdir.write('abc/2.txt', b'')
        tempdir.write('abc/def/md.yml', b'')
        tempdir.write('abc/def/3.txt', b'')
        tempdir.write('excluded/md.yml', b'')
        tempdir.write('excluded/4.txt', b'')
        tempdir.write('excluded/xyz/md.yml', b'')
        tempdir.write('excluded/xyz/5.txt', b'')
        tempdir.write('ignored/.ignored', b'')
        tempdir.write('ignored/xyz/5.txt', b'')
        tempdir.write('123/md.yml', b'')
        tempdir.write('123/xyz/md.yml', b'')
        tempdir.write('123/ignored/ignored.yml', b'')
        tempdir.write('123/ignored/md.yml', b'')
        os.chdir(tempdir.path)

        f =  files.get_files('.txt', tempdir.path, ['excluded'], '.ignored')
        assert (f == [os.path.join('1.txt'),
                      os.path.join('abc', '2.txt'),
                      os.path.join('abc', 'def', '3.txt')])

        f =  files.get_files('md.yml', tempdir.path, ['excluded'], 'ignored.yml')
        assert (f == [os.path.join('md.yml'),
                      os.path.join('123','md.yml'),
                      os.path.join('123','xyz','md.yml'),
                      os.path.join('abc','def','md.yml')])


class Test_metadata_info(object):
    # noinspection PyProtectedMember
    def test_multiple_files(self, tempdir):
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
        res = [os.path.join('metadata.yml'), os.path.join('abc', 'metadata.yml')]
        assert(files.get_metadata_files(tempdir.path)==res)


