import os
from glob import glob
from setuptools import setup

pjoin = os.path.join

setup(name='datalabframework',
      version='0.2.7',
      install_requires=[
        'requests',
        'pyyaml',
        'jinja2',
        'jupyter'
      ],
      description='Productivity Utilities for Data Science with Python Notebooks',
      url='http://github.com/natbusa/datalabframework',
      author='Natalino Busa',
      author_email='natalino.busa@gmail.com',
      license='MIT',
      packages=['datalabframework'],
      scripts = glob(pjoin('scripts', '*')),
      zip_safe=False,
      platforms       = "Linux, Mac OS X, Windows",
      keywords        = ['Interactive', 'Interpreter', 'Shell', 'Web'],
      classifiers     = [
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research']
)
