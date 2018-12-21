import os
from datalabframework._utils import Singleton

class Paths(metaclass=Singleton):

    def get_rootdir(self, workdir, rootfiles):
        for filename in rootfiles:
            path = workdir
            while True:
                try:
                    ls = os.listdir(path)
                    if filename in ls:
                        return os.path.abspath(path)
                    else:
                        path += '/..'
                except:
                    break

        # nothing found: using the current working dir
        return workdir

    def set_paths(self, path):
        try:
            os.chdir(path)
        except:
            pass
        finally:
            self._workdir = os.getcwd()
            self._rootdir = self.get_rootdir(self._workdir, self._rootfiles)

    def __init__(self):
            self._rootfiles = ['__main__.py']
            self._workdir = None
            self._rootdir = None

            self.set_paths(os.getcwd())

    def workdir(self,path=None):
        if path is not None:
            self.set_paths(path)

        return self._workdir

    def rootdir(self):
        return self._rootdir

def rootdir():
    return Paths().rootdir()

def workdir(path=None):
    return Paths().workdir(path)

