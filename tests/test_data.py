from datalabframework import data

import os
import yaml
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
