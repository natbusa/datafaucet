from datalabframework import utils

import os
import yaml
from textwrap import dedent

import pytest
from testfixtures import TempDirectory

from datalabframework.utils import os_sep


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

def test_breadcrumb_path():
    assert(utils.breadcrumb_path(os.sep)=='.')
    assert(utils.breadcrumb_path(os_sep('/aaa'))=='.aaa')
    assert(utils.breadcrumb_path(os_sep('/oh/aaa/123'))=='.oh.aaa.123')
    assert(utils.breadcrumb_path(os_sep('/oh/aaa/123'), os_sep('/la'))=='.oh.aaa.123')
    assert(utils.breadcrumb_path(os_sep('/oh/aaa/123'), os_sep('/oh'))=='.aaa.123')
    assert(utils.breadcrumb_path(os_sep('/oh/ww/aaa/123'), os_sep('/oh'))=='.ww.aaa.123')
    assert(utils.breadcrumb_path(os_sep('/oh/ww/aaa/123'), os_sep('/oh/'))=='.ww.aaa.123')

def test_relative_filename():
    assert(utils.relative_filename(os_sep('/aaa'))=='aaa')
    assert(utils.relative_filename('aaa')=='aaa')

    assert(utils.relative_filename(os_sep('/aaa'), os_sep('/the/rootpath'))=='aaa') # should return error?
    assert(utils.relative_filename(os_sep('/aaa/dd/s'), os_sep('/the/rootpath'))==os_sep('aaa/dd/s')) # should return error?
    assert(utils.relative_filename('aaa', os_sep('/the/rootpath'))=='aaa')  # should return error?

    assert(utils.relative_filename(os_sep('/the/rootpath/abc/aaa'), os_sep('/the/rootpath'))==os_sep('abc/aaa'))
    assert(utils.relative_filename(os_sep('/the/rootpath/aaa'), os_sep('/the/rootpath'))==os_sep('aaa'))

def test_absolute_filename():
    assert(utils.absolute_filename(os_sep('/aaa'))== os_sep('/aaa'))
    assert(utils.absolute_filename('aaa')==os_sep('./aaa'))

    assert(utils.absolute_filename(os_sep('/aaa'), os_sep('/the/rootpath'))==os_sep('/aaa'))
    assert(utils.absolute_filename('aaa', os_sep('/the/rootpath'))==os_sep('/the/rootpath/aaa'))

    assert(utils.absolute_filename(os_sep('/the/rootpath/abc/aaa'), os_sep('/the/rootpath'))==os_sep('/the/rootpath/abc/aaa'))
    assert(utils.absolute_filename(os_sep('/the/rootpath/aaa'), os_sep('/the/rootpath'))==os_sep('/the/rootpath/aaa'))

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
        assert(l==['1.txt', os_sep('abc/2.txt'), os_sep('abc/def/3.txt')])
        l = utils.get_project_files('.txt', '.', ['excluded'], '.ignored', False)
        assert(l==[os_sep('./1.txt'), os_sep('./abc/2.txt'), os_sep('./abc/def/3.txt')])
        l = utils.get_project_files('.txt', dir.path, ['excluded'], '.ignored', True)
        assert(l==['1.txt', os_sep('abc/2.txt'), os_sep('abc/def/3.txt')])
        l = utils.get_project_files('.txt', dir.path, ['excluded'], '.ignored', False)
        assert(l==[os.path.join(dir.path, '1.txt'), os.path.join(dir.path, 'abc', '2.txt'), os.path.join(dir.path, 'abc', 'def', '3.txt')])

        l = utils.get_project_files('md.yml', '.', ['excluded'], 'ignored.yml', True)
        assert(l== ['md.yml',os_sep('123/md.yml'),os_sep('123/xyz/md.yml'),os_sep('abc/def/md.yml')])
        l = utils.get_project_files('md.yml', '.', ['excluded'], 'ignored.yml', False)
        assert(l== [os_sep('./md.yml'), os_sep('./123/md.yml'), os_sep('./123/xyz/md.yml'), os_sep('./abc/def/md.yml')])


def test_render():
    doc = '''
        ---
        default:
            run: default
            resources:
                input:
                    path: datasets/extract/{{ default.run }}
                    format: parquet
                    provider: local-other
        test:
            run: test
            resources:
                oh : '{{ default.run }}'
                data:
                    path: datasets/extract/{{ test.resources.oh }}
                    format: parquet-{{ test.resources.data.provider }}
                    provider: local-{{ test.resources.data.path }}
        '''

    ref =  {'default': {
                'resources': {
                    'input': {
                        'format': 'parquet',
                        'path': 'datasets/extract/default',
                        'provider': 'local-other'}},
                'run': 'default'},
            'test': {
                'resources': {
                    'data': {
                        'format': 'parquet-local-datasets/extract/default',
                        'path': 'datasets/extract/default',
                        'provider': 'local-datasets/extract/default'},
                        'oh': 'default'},
                'run': 'test'}}

    metadata = yaml.load(dedent(doc))
    res = utils.render(metadata)
    assert(res==ref)
