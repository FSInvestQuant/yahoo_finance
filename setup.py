#!/usr/bin/env python

import sys
try:
  from setuptools import setup, find_packages
except ImportError:
  from distutils.core import setup, find_packages

import yahoo_finance

setup(
    name='yahoo_finance',
    version=yahoo_finance.__version__,
    description='Script for basic access to yahoo finance data.',
    long_description=yahoo_finance.__doc__,
    author=yahoo_finance.__author__,

    py_modules=['yahoo_finance'],
    scripts=['yahoo_finance.py'],
    packages=find_packages(),

    license='MIT',
    platforms='any',
)