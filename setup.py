#! /usr/bin/env python

from distutils.core import setup
from distutils.extension import Extension

setup(
    name='simhash_db',
    version='0.1.0',
    description='''
        Near-Duplicate Detection with Simhash in Databases.
        Fork from Moz with addition of Elasticsearch and Postgres''',
    url='https://github.com/Sagacify/simhash-db-py',
    author='Kevin Françoisse',
    author_email='info@sagacify.com',
    packages=['simhash_db'],
    package_dir={'simhash_db': 'simhash_db'},
    classifiers=[
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP'
    ],
)
