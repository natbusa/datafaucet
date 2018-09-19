from datalabframework import project

import os
import pytest
from testfixtures import TempDirectory

@pytest.fixture()
def dir():
    with TempDirectory() as dir:
        original_dir = os.getcwd()
        os.chdir(dir.path)

        p = project.Config()
        p.__class__._instances={};

        project.Config(dir.path)
        yield dir
        os.chdir(original_dir)

class Test_init(object):
    def test_init(self):

        p = project.Config()
        p.__class__._instances={};
        p = project.Config()

        assert(p._rootpath==os.getcwd())
        assert(p._workdir==os.getcwd())

    def test_init_params(self,dir):
        subdir = dir.makedir('abc')
        dir.write('__main__.py', b'')
        dir.write('abc/test.ipynb', b'')

        p = project.Config()
        p.__class__._instances={};
        p = project.Config(subdir, os.path.join(subdir, 'test.ipynb'))

        assert(p._rootpath==dir.path)
        assert(p._filename==os.path.join(subdir,'test.ipynb'))
        assert(p._workdir==subdir)

class Test_rootpath(object):
    def test_emptydir(self, dir):
        project.Config()._workdir = None
        assert(project.rootpath()==dir.path)

    def test_main(self, dir):
        dir.write('__main__.py', b'')
        dir.write('test.123', b'')
        assert(project.rootpath()==dir.path)

    def test_submodule(self, dir):
        subdir = dir.makedir('abc')
        dir.write('__main__.py', b'')
        dir.write('test.123', b'')
        dir.write('abc/__init__.py', b'')
        os.chdir(subdir)
        assert(project.rootpath()==dir.path)

def test_find_notebook(dir):
    subdir = dir.makedir('abc')
    dir.write('foo.ipynb', b'')
    dir.write('foo bar.ipynb', b'')
    dir.write('abc/bar.ipynb', b'')
    assert(project._find_notebook('foo', [dir.path])==os.path.join(dir.path, 'foo.ipynb'))
    assert(project._find_notebook('foo')=='foo.ipynb')
    assert(project._find_notebook('abc.bar', ['abc'])==os.path.join('abc', 'bar.ipynb'))
    assert(project._find_notebook('foo_bar')=='foo bar.ipynb')
