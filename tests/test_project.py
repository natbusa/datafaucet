from datalabframework import project

import os
import pytest
from testfixtures import TempDirectory

@pytest.fixture()
def dir():
    with TempDirectory() as dir:
        original_dir = os.getcwd()
        os.chdir(dir.path)
        yield dir
        os.chdir(original_dir)

class Test_rootpath(object):
    def test_emptydir(self, dir):
        assert(project.rootpath()==dir.path)
        assert(project.rootpath('aaa')==dir.path)

    def test_main(self, dir):
        dir.write('__main__.py', b'')
        dir.write('test.123', b'')
        assert(project.rootpath()==dir.path)
        assert(project.rootpath('test.123')==dir.path)

    def test_submodule(self, dir):
        subdir = dir.makedir('abc')
        dir.write('__main__.py', b'')
        dir.write('test.123', b'')
        dir.write('abc/__init__.py', b'')
        os.chdir(subdir)
        assert(project.rootpath()==dir.path)
        assert(project.rootpath('test.123')==dir.path)

def test_find_notebook(dir):
    subdir = dir.makedir('abc')
    dir.write('foo.ipynb', b'')
    dir.write('foo bar.ipynb', b'')
    dir.write('abc/bar.ipynb', b'')
    assert(project.find_notebook('foo', [dir.path])==dir.path+'/foo.ipynb')
    assert(project.find_notebook('foo')=='foo.ipynb')
    assert(project.find_notebook('abc.bar', ['abc'])=='abc/bar.ipynb')
    assert(project.find_notebook('foo_bar')=='foo bar.ipynb')
