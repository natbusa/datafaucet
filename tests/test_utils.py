from datalabframework import utils

import os
import pytest
from testfixtures import TempDirectory

def test_lrchop():
    s = utils.lrchop('asd12345aaa','asd','aaa')
    assert(s=='12345')

    s = utils.lrchop('asd12345aaa',b='asd')
    assert(s=='12345aaa')

    s = utils.lrchop('asd12345aaa',e='aaa')
    assert(s=='asd12345')

    s = utils.lrchop('asd12345aaa')
    assert(s=='asd12345aaa')

def test_merge():
    a = {'a':1, 'b':4, 'c':{'merge1':2}}
    b = {'d':'add', 'b':'override', 'c':{'merge2':4}}
    r1 = utils.merge(a,b)
    r2 = {'a': 1, 'd': 'add', 'b': 'override', 'c': {'merge2': 4, 'merge1': 2}}
    assert(r1==r2)

def test_relative_filename():
    assert(utils.relative_filename('/aaa')=='aaa')
    assert(utils.relative_filename('aaa')=='aaa')

    assert(utils.relative_filename('/aaa', '/the/rootpath')=='aaa')
    assert(utils.relative_filename('aaa', '/the/rootpath')=='aaa')

    assert(utils.relative_filename('/the/rootpath/abc/aaa', '/the/rootpath')=='abc/aaa')
    assert(utils.relative_filename('/the/rootpath/aaa', '/the/rootpath')=='aaa')

def test_absolute_filename():
    assert(utils.absolute_filename('/aaa')=='/aaa')
    assert(utils.absolute_filename('aaa')=='./aaa')

    assert(utils.absolute_filename('/aaa', '/the/rootpath')=='/aaa')
    assert(utils.absolute_filename('aaa', '/the/rootpath')=='/the/rootpath/aaa')

    assert(utils.absolute_filename('/the/rootpath/abc/aaa', '/the/rootpath')=='/the/rootpath/abc/aaa')
    assert(utils.absolute_filename('/the/rootpath/aaa', '/the/rootpath')=='/the/rootpath/aaa')

def test_get_project_files():
    with TempDirectory() as dir:
        dir.makedir('abc')
        dir.makedir('abc/def')
        dir.makedir('excluded')
        dir.makedir('excluded/xyz')
        dir.makedir('ignored')
        dir.makedir('ignored/xyz')
        dir.makedir('123')
        dir.makedir('123/xyz')
        dir.makedir('123/ignored')
        dir.write('md.yml', b'')
        dir.write('1.txt', b'')
        dir.write('abc/2.txt', b'')
        dir.write('abc/def/md.yml', b'')
        dir.write('abc/def/3.txt', b'')
        dir.write('excluded/md.yml', b'')
        dir.write('excluded/4.txt', b'')
        dir.write('excluded/xyz/md.yml', b'')
        dir.write('excluded/xyz/5.txt', b'')
        dir.write('ignored/.ignored', b'')
        dir.write('ignored/xyz/5.txt', b'')
        dir.write('123/md.yml', b'')
        dir.write('123/xyz/md.yml', b'')
        dir.write('123/ignored/ignored.yml', b'')
        dir.write('123/ignored/md.yml', b'')
        os.chdir(dir.path)

        l = utils.get_project_files('.txt', '.', ['excluded'], '.ignored', True)
        assert(l==['1.txt', 'abc/2.txt', 'abc/def/3.txt'])
        l = utils.get_project_files('.txt', '.', ['excluded'], '.ignored', False)
        assert(l==['./1.txt', './abc/2.txt', './abc/def/3.txt'])
        l = utils.get_project_files('.txt', dir.path, ['excluded'], '.ignored', True)
        assert(l==['1.txt', 'abc/2.txt', 'abc/def/3.txt'])
        l = utils.get_project_files('.txt', dir.path, ['excluded'], '.ignored', False)
        assert(l==[dir.path+'/1.txt', dir.path+'/abc/2.txt', dir.path+'/abc/def/3.txt'])

        l = utils.get_project_files('md.yml', '.', ['excluded'], 'ignored.yml', True)
        assert(l== ['md.yml','123/md.yml','123/xyz/md.yml','abc/def/md.yml'])
        l = utils.get_project_files('md.yml', '.', ['excluded'], 'ignored.yml', False)
        assert(l== ['./md.yml','./123/md.yml','./123/xyz/md.yml','./abc/def/md.yml'])
