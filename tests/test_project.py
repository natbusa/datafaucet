from datalabframework import project

import os
import pytest
from testfixtures import TempDirectory

@pytest.fixture()
def dir():
    with TempDirectory() as dir:
        os.chdir(dir.path)
        yield dir

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
