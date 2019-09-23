import os

import pytest
from testfixtures import TempDirectory

from datafaucet import _project
from datafaucet import project
from datafaucet._utils import Singleton
from datafaucet import paths


@pytest.fixture()
def tempdir():
    with TempDirectory() as d:
        original_dir = os.getcwd()
        os.chdir(d.path)

        # clear all Singletons at the beginning of the test
        Singleton._instances = {}

        # set rootdir
        paths.set_rootdir(d.path)

        yield d
        os.chdir(original_dir)

class Test_init(object):
    # noinspection PyProtectedMember,PyProtectedMember
    def test_init(self, tempdir):
        tempdir.write('__main__.py', b'')
        keys = list(project.config().keys())
        assert ( keys == ['version', 'python_version', 'profile', 'filename', 'rootdir', 'workdir', 'username', 'repository', 'files', 'engine'])
        assert (project.config()['rootdir'] == tempdir.path)

    # noinspection PyProtectedMember,PyProtectedMember,PyProtectedMember
    def test_init_params(self, tempdir):
        subdir = tempdir.makedir('abc')

        tempdir.write('__main__.py', b'')
        tempdir.write('abc/test.ipynb', b'')

        p = _project.Config(filename='abc/test.ipynb')

        assert (p.profile() is None)
        assert (p.config()['filename'] == 'abc/test.ipynb')
        assert (p.config()['rootdir'] == tempdir.path)


class Test_rootdir(object):
    def test_emptydir(self, tempdir):
        p = _project.Config()
        assert (p.config()['workdir'] == tempdir.path)
        assert (p.config()['rootdir'] == tempdir.path)

    def test_main(self, tempdir):
        tempdir.write('__main__.py', b'')
        tempdir.write('test.123', b'')
        assert (project.config()['workdir'] == tempdir.path)
        assert (project.config()['rootdir'] == tempdir.path)

    def test_submodule(self, tempdir):
        subdir = tempdir.makedir('abc')
        tempdir.write('__main__.py', b'')
        tempdir.write('test.123', b'')
        tempdir.write('abc/__init__.py', b'')
        os.chdir(subdir)
        p = _project.Config()
        assert (p.config()['workdir'] == subdir)
        assert (p.config()['rootdir'] == tempdir.path)
