import re

from io import open
from setuptools import setup, find_packages

README = 'README.rst'
CHANGES = 'CHANGES.rst'
VERSION_FILE = 'comparator/__init__.py'


def read(path):
    with open(path, encoding='utf-8') as f:
        return f.read()


def find_version():
    version_file = read(VERSION_FILE)
    version_match = re.search(
        r"^__version__ = ['\"]([^'\"]*)['\"]",
        version_file,
        re.M)
    if version_match:
        return version_match.group(1)

    raise RuntimeError("Unable to find version string.")


setup(
    name='comparator',
    version=find_version(),
    author='Aaron Biller',
    author_email='aaronbiller@gmail.com',
    description='Utility for comparing results between data sources',
    long_description=read(README) + '\n' + read(CHANGES),
    license='Apache 2.0',
    keywords='utility compare database',
    url='https://github.com/aaronbiller/comparator',
    packages=find_packages(),
    tests_require=[
        'pytest',
        'pytest-cov',
        'mock',
        'spackl'
    ],
    install_requires=[
        'future==0.18.3',
        'google-cloud-bigquery==1.5.0',
        'psycopg2-binary==2.7.5',
        'PyYAML',
        'SQLAlchemy==1.2.11',
        'sqlalchemy-redshift==0.7.1',
        'pandas>=0.22.0',
        'pytest-runner==4.2',
    ],
    extras_require={
        ':python_version == "2.7"': [
            'pathlib2==2.3.2',
        ],
    },
    include_package_data=True,
    scripts=[],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Database',
        'Topic :: Utilities',
    ],
)
