from datalabframework import notebook

import os
from textwrap import dedent

import pytest
from testfixtures import TempDirectory

@pytest.fixture()
def dir():
    with TempDirectory() as dir:
        yield dir

class Test_underscore_get_filename(object):
    def test_minimal(self, dir):
        assert(notebook._get_filename() in ['notebook.py', 'notebook.pyc'])
#
# class Test_get_filename(object):
#     def test_minimal(self, dir):
#         assert(notebook.get_filename() in ['notebook.py', 'notebook.pyc'])

def test_statistics(dir):
    with open('./tests/notebooks/a.ipynb', 'rb') as input:
          dir.write('a.ipynb', input.read())
          dir.write('__main__.py',b'')
          os.chdir(dir.path)
          res = {'cells': 25,
                 'code': 12,
                 'ename': u'KeyError',
                 'evalue': u"'engines'",
                 'executed': 12,
                 'markdown': 13}
          assert(notebook.statistics('a.ipynb')==res)
