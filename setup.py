# -*- coding: utf-8 -*-
"""
    Setup file for pydeequ.
    Use setup.cfg to configure your project.

    This file was generated with PyScaffold 3.2.3.
    PyScaffold helps you to put up the scaffold of your new Python project.
    Learn more under: https://pyscaffold.org/
"""
import sys
import os

from pkg_resources import VersionConflict, require
from setuptools import setup

try:
    require('setuptools>=38.3')
except VersionConflict:
    print("Error: version of setuptools is too old (<38.3)!")
    sys.exit(1)

def setup_package():
    needs_sphinx = {'build_sphinx', 'upload_docs'}.intersection(sys.argv)
    sphinx = ['sphinx'] if needs_sphinx else []

    setup(setup_requires=['six', 'pyscaffold>=2.5a0,<2.6a0'] + sphinx,
          use_pyscaffold=True)

if __name__ == "__main__":
    setup_package()
