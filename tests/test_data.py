import os

import pytest
from testfixtures import TempDirectory

@pytest.fixture()
def tempdir():
    with TempDirectory() as dir:
        original_dir = os.getcwd()
        os.chdir(dir.path)
        yield dir
        os.chdir(original_dir)
