from io import open
from setuptools import setup, find_packages

with open('README.rst', encoding='utf-8') as f:
    readme = f.read()

with open('CHANGES.rst', encoding='utf-8') as f:
    changes = f.read()


setup(
    name='comparator',
    version='0.3.2',
    author='Aaron Biller',
    author_email='aaronbiller@gmail.com',
    description='Utility for comparing results between data sources',
    long_description=readme + '\n' + changes,
    license='Apache 2.0',
    keywords='utility compare database',
    url='https://github.com/aaronbiller/comparator',
    packages=find_packages(),
    tests_require=[
        'pytest',
        'pytest-cov',
        'mock',
    ],
    install_requires=[
        'future==0.16.0',
        'google-cloud-bigquery==1.5.0',
        'psycopg2-binary==2.7.5',
        'PyYAML',
        'SQLAlchemy==1.2.11',
        'sqlalchemy-redshift==0.7.1',
        'pandas==0.23.4',
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
