from datalabframework import utils
from datalabframework.utils import os_sep

import os
from textwrap import dedent
from testfixtures import TempDirectory

from ruamel.yaml import YAML

yaml =  YAML()
yaml.preserve_quotes = True
yaml.indent(mapping=4, sequence=4, offset=2)


def test_lrchop():
    s = utils.lrchop('asd12345aaa', 'asd', 'aaa')
    assert (s == '12345')

    s = utils.lrchop('asd12345aaa', b='asd')
    assert (s == '12345aaa')

    s = utils.lrchop('asd12345aaa', e='aaa')
    assert (s == 'asd12345')

    s = utils.lrchop('asd12345aaa')
    assert (s == 'asd12345aaa')


def test_merge():
    a = {'a': 1, 'b': 4, 'c': {'merge1': 2}}
    b = {'d': 'add', 'b': 'override', 'c': {'merge2': 4}}
    r1 = utils.merge(a, b)
    r2 = {'a': 1, 'd': 'add', 'b': 'override', 'c': {'merge2': 4, 'merge1': 2}}
    assert (r1 == r2)


def test_breadcrumb_path():
    assert (utils.breadcrumb_path(os.sep) == '.')
    assert (utils.breadcrumb_path(os_sep('/aaa')) == '.aaa')
    assert (utils.breadcrumb_path(os_sep('/oh/aaa/123')) == '.oh.aaa.123')
    assert (utils.breadcrumb_path(os_sep('/oh/aaa/123'), os_sep('/la')) == '.oh.aaa.123')
    assert (utils.breadcrumb_path(os_sep('/oh/aaa/123'), os_sep('/oh')) == '.aaa.123')
    assert (utils.breadcrumb_path(os_sep('/oh/ww/aaa/123'), os_sep('/oh')) == '.ww.aaa.123')
    assert (utils.breadcrumb_path(os_sep('/oh/ww/aaa/123'), os_sep('/oh/')) == '.ww.aaa.123')


def test_relative_filename():
    assert (utils.relative_filename(os_sep('/aaa')) == 'aaa')
    assert (utils.relative_filename('aaa') == 'aaa')

    assert (utils.relative_filename(os_sep('/aaa'), os_sep('/the/rootpath')) == 'aaa')  # should return error?
    assert (utils.relative_filename(os_sep('/aaa/dd/s'), os_sep('/the/rootpath')) == os_sep(
        'aaa/dd/s'))  # should return error?
    assert (utils.relative_filename('aaa', os_sep('/the/rootpath')) == 'aaa')  # should return error?

    assert (utils.relative_filename(os_sep('/the/rootpath/abc/aaa'), os_sep('/the/rootpath')) == os_sep('abc/aaa'))
    assert (utils.relative_filename(os_sep('/the/rootpath/aaa'), os_sep('/the/rootpath')) == os_sep('aaa'))


def test_absolute_filename():
    assert (utils.absolute_filename(os_sep('/aaa')) == os_sep('/aaa'))
    assert (utils.absolute_filename('aaa') == os_sep('./aaa'))

    assert (utils.absolute_filename(os_sep('/aaa'), os_sep('/the/rootpath')) == os_sep('/aaa'))
    assert (utils.absolute_filename('aaa', os_sep('/the/rootpath')) == os_sep('/the/rootpath/aaa'))

    assert (utils.absolute_filename(os_sep('/the/rootpath/abc/aaa'), os_sep('/the/rootpath')) == os_sep(
        '/the/rootpath/abc/aaa'))
    assert (utils.absolute_filename(os_sep('/the/rootpath/aaa'), os_sep('/the/rootpath')) == os_sep(
        '/the/rootpath/aaa'))


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

        f =  utils.get_project_files('.txt', '.', ['excluded'], '.ignored', True)
        assert (f == ['1.txt', os_sep('abc/2.txt'), os_sep('abc/def/3.txt')])
        f =  utils.get_project_files('.txt', '.', ['excluded'], '.ignored', False)
        assert (f == [os_sep('./1.txt'), os_sep('./abc/2.txt'), os_sep('./abc/def/3.txt')])
        f =  utils.get_project_files('.txt', dir.path, ['excluded'], '.ignored', True)
        assert (f == ['1.txt', os_sep('abc/2.txt'), os_sep('abc/def/3.txt')])
        f =  utils.get_project_files('.txt', dir.path, ['excluded'], '.ignored', False)
        assert (f == [os.path.join(dir.path, '1.txt'), os.path.join(dir.path, 'abc', '2.txt'),
                      os.path.join(dir.path, 'abc', 'def', '3.txt')])

        f =  utils.get_project_files('md.yml', '.', ['excluded'], 'ignored.yml', True)
        assert (f == ['md.yml', os_sep('123/md.yml'), os_sep('123/xyz/md.yml'), os_sep('abc/def/md.yml')])
        f =  utils.get_project_files('md.yml', '.', ['excluded'], 'ignored.yml', False)
        assert (f == [os_sep('./md.yml'), os_sep('./123/md.yml'), os_sep('./123/xyz/md.yml'),
                      os_sep('./abc/def/md.yml')])


def test_render():
    doc = '''
        ---
        profile: default
        resources:
            input:
                path: datasets/extract/{{ profile }}
                format: parquet
                provider: local-other
        '''

    ref = {
        'resources': {
            'input': {
                'format': 'parquet',
                'path': 'datasets/extract/default',
                'provider': 'local-other'}},
        'profile': 'default'}

    metadata = yaml.load(dedent(doc))
    res = utils.render(metadata)
    assert (res == ref)

def test_render_multipass():

    # no multipass currently

    doc = '''
        ---
        profile: test
        resources:
            oh : '{{ profile }}'
            data:
                path: datasets/extract/{{ resources.oh }}
                format: parquet-{{ resources.oh }}
                provider: local-{{ resources.oh }}
        '''
    ref = {
         'resources': {
             'data': {
                 'format': 'parquet-test',
                 'path': 'datasets/extract/test',
                 'provider': 'local-test'},
            'oh': 'test'},
        'profile': 'test'}

    metadata = yaml.load(dedent(doc))
    res = utils.render(metadata)
    assert (res == ref)


def test_render_env():

    os.environ['MYENVVAR']='/bin/bash'
    
    doc = '''
        ---
        profile: default
        variables:
            ref: default.variables.a0
            a0: "{{ env('MYENVVAR') }}"
            c0: "{{ ''|env('MYENVVAR')}}"
        
            a1: "{{ env('MYENVVAR','default_value') }}"
            c1: "{{ 'default_value'|env('MYENVVAR')}}"
            
            a2: "{{ env('UNDEFINED_ENV', 'world') }}"
            c2: "{{ 'world'|env('UNDEFINED_ENV')}}"
        '''

    ref = { 'profile': 'default',
            'variables': {
                'a0': '/bin/bash',
                'a1': '/bin/bash',
                'a2': 'world',
                'c0': '/bin/bash',
                'c1': '/bin/bash',
                'c2': 'world',
                'ref': 'default.variables.a0'}}

    metadata = yaml.load(dedent(doc))
    res = utils.render(metadata)
    assert (res == ref)

def test_render_multipass_concat():
    
    os.environ['MYENVVAR']='/bin/bash'
    doc = '''
        ---
        variables:
            a: "hello-{{ env('NOTFOUND_DEFAULT_VALUE', 'world') }}"
            b: "one-{{ env('MYENVVAR') }}"
            c: "two-{{ variables.b }}"
            d: "three-{{ variables.c }}"
            e: "{{ variables.c + '-plus-' + variables.d }}"
        '''

    ref = { 'variables': {
            'a': 'hello-world',
            'b': 'one-/bin/bash',
            'c': 'two-one-/bin/bash',
            'd': 'three-two-one-/bin/bash',
            'e': 'two-one-/bin/bash-plus-three-two-one-/bin/bash'}}

    metadata = yaml.load(dedent(doc))
    res = utils.render(metadata)
    assert (res == ref)
