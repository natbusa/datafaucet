from datalabframework import project

import os
import pytest
from testfixtures import TempDirectory


@pytest.fixture()
def tempdir():
    with TempDirectory() as d:
        original_dir = os.getcwd()
        os.chdir(d.path)

        p = project.Config()
        p.__class__._instances = {}

        project.Config(d.path)
        yield d
        os.chdir(original_dir)


class Test_init(object):
    # noinspection PyProtectedMember,PyProtectedMember
    def test_init(self):
        p = project.Config()
        p.__class__._instances = {}
        p = project.Config()

        assert (p._rootpath == os.getcwd())
        assert (p._workdir == os.getcwd())

    # noinspection PyProtectedMember,PyProtectedMember,PyProtectedMember
    def test_init_params(self, tempdir):
        subdir = tempdir.makedir('abc')
        tempdir.write('__main__.py', b'')
        tempdir.write('abc/test.ipynb', b'')

        p = project.Config()
        p.__class__._instances = {}
        p = project.Config(subdir, os.path.join(subdir, 'test.ipynb'))

        assert (p._rootpath == tempdir.path)
        assert (p._filename == os.path.join(subdir, 'test.ipynb'))
        assert (p._workdir == subdir)


class Test_rootpath(object):
    def test_emptydir(self, tempdir):
        project.Config()._workdir = None
        assert (project.rootpath() == tempdir.path)

    def test_main(self, tempdir):
        tempdir.write('__main__.py', b'')
        tempdir.write('test.123', b'')
        assert (project.rootpath() == tempdir.path)

    def test_submodule(self, tempdir):
        subdir = tempdir.makedir('abc')
        tempdir.write('__main__.py', b'')
        tempdir.write('test.123', b'')
        tempdir.write('abc/__init__.py', b'')
        os.chdir(subdir)
        assert (project.rootpath() == tempdir.path)


# noinspection PyProtectedMember,PyProtectedMember,PyProtectedMember,PyProtectedMember
def test_find_notebook(tempdir):
    tempdir.write('foo.ipynb', b'')
    tempdir.write('foo bar.ipynb', b'')
    tempdir.write('abc/bar.ipynb', b'')
    assert (project._find_notebook('foo', [tempdir.path]) == os.path.join(tempdir.path, 'foo.ipynb'))
    assert (project._find_notebook('foo') == 'foo.ipynb')
    assert (project._find_notebook('abc.bar', ['abc']) == os.path.join('abc', 'bar.ipynb'))
    assert (project._find_notebook('foo_bar') == 'foo bar.ipynb')
