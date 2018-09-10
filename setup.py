#!/usr/bin/env python3
from setuptools import setup, find_packages

from comparator import __version__

install_requires = [
    line.strip() for line in open('requirements.txt').readlines()]


setup(
    name='comparator',
    version=__version__,
    author='Aaron Biller',
    author_email='aaronbiller@gmail.com',
    description='Utility for comparing results between data sources',
    license='Apache 2.0',
    keywords='utility compare database',
    url='https://github.com/aaronbiller/comparator',
    packages=find_packages(),
    tests_require=[
        'pytest',
        'pytest-cov',
        'pytest-timeout',
        'mock>=1.0.1',
    ],
    install_requires=install_requires,
    include_package_data=True,
    scripts=[],
    classifiers=[
        'Topic :: Database',
        'Topic :: Utilities',
    ],
)
