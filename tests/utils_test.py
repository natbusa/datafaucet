from datalabframework import utils

from testfixtures import TempDirectory
import pytest
import os

@pytest.fixture()
def dir():
    with TempDirectory() as dir:
        os.chdir(dir.path)
        yield dir

def test_lrchop():
    s = utils.lrchop('asd12345aaa','asd','aaa')
    assert(s=='12345')

    s = utils.lrchop('asd12345aaa',b='asd')
    assert(s=='12345aaa')

    s = utils.lrchop('asd12345aaa',e='aaa')
    assert(s=='asd12345')

    s = utils.lrchop('asd12345aaa')
    assert(s=='asd12345aaa')

class Test_relative_filename(object):
    def test_emptydir(self, dir):
        assert(utils.relative_filename('/aaa')=='aaa')
        assert(utils.relative_filename('aaa')=='aaa')

    def test_main(self, dir):
        dir.write('__main__.py', '')
        dir.write('test.ipynb', 'test')
        assert(utils.relative_filename(dir.path+'/test.ipynb')=='test.ipynb')
        assert(utils.relative_filename('test.ipynb')=='test.ipynb')

    def test_submodule(self, dir):
        subdir = dir.makedir('abc')
        dir.write('__main__.py', '')
        dir.write('abc/__init__.py', '')
        dir.write('abc/test.ipynb', 'test')
        os.chdir(subdir)
        assert(utils.relative_filename(subdir+'/test.ipynb')=='abc/test.ipynb')
        assert(utils.relative_filename('test.ipynb')=='test.ipynb')

class Test_absolute_filename(object):
    def test_emptydir(self, dir):
        assert(utils.absolute_filename('/aaa')=='/aaa')
        assert(utils.absolute_filename('aaa')==dir.path+'/aaa')

    def test_main(self, dir):
        dir.write('__main__.py', '')
        dir.write('test.ipynb', 'test')
        assert(utils.absolute_filename(dir.path+'/test.ipynb')==dir.path+'/test.ipynb')
        assert(utils.absolute_filename('test.ipynb')==dir.path+'/test.ipynb')

    def test_submodule(self, dir):
        subdir = dir.makedir('abc')
        dir.write('__main__.py', '')
        dir.write('abc/__init__.py', '')
        dir.write('abc/test.ipynb', 'test')
        os.chdir(subdir)
        assert(utils.absolute_filename('abc/test.ipynb')==subdir+'/test.ipynb')
