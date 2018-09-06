from datalabframework import notebook, project

import os
from textwrap import dedent

import pytest
from testfixtures import TempDirectory

@pytest.fixture()
def dir():
    with TempDirectory() as dir:
        original_dir = os.getcwd()
        os.chdir(dir.path)
        yield dir
        os.chdir(original_dir)

def test_statistics():
    dir = TempDirectory()
    original_dir = os.getcwd()
    with open('tests/notebooks/a.ipynb', 'rb') as input:
        dir.write('a.ipynb', input.read())
        dir.write('__main__.py',b'')
        res = {'cells': 25,
             'code': 12,
             'ename': u'KeyError',
             'evalue': u"'engines'",
             'executed': 12,
             'markdown': 13}
        os.chdir(dir.path)
        assert(notebook.statistics('a.ipynb')==res)
    os.chdir(original_dir)
    dir.cleanup()
