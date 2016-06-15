#! /usr/bin/env python

from distutils.core import setup
from distutils.extension import Extension

setup(name           = 'simhash_db',
    version          = '0.1.0',
    description      = 'Near-Duplicate Detection with Simhash in Databases',
    url              = 'https://github.com/Sagacify/simhash-db-py',
    author           = 'Dan Lecocq',
    author_email     = 'dan@moz.com',
    packages         = ['simhash_db'],
    package_dir      = {'simhash_db': 'simhash_db'},
    dependencies     = [],
    install_requires = [
        "elasticsearch >= 2.1.0"
    ],
    dependency_links = [
        'git+https://github.com/Sagacify/simhash-py.git'
    ],
    classifiers      = [
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP'
    ],
)
