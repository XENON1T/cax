#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

PROJECT = 'cax'
VERSION = '2.1.0'

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'checksumdir', 'scp', 'pagerduty-api', 'pymongo', 'paramiko',
    'numpy'
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='cax',
    version=VERSION,
    description="Copying Around XENON1T data",
    long_description=readme + '\n\n' + history,
    author="Christopher Tunnell",
    author_email='ctunnell@nikhef.nl',
    url='https://github.com/tunnell/cax',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    data_files=[('cax', ['cax/cax.json'])],
    license="ISCL",
    zip_safe=False,
    keywords='cax',
    classifiers=[
        'Development Status :: 3 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    entry_points={
        'console_scripts': [
            'cax = cax.main:main',
            'caxer = cax.main:main',  # For uniformity with paxer
            'cax-process = cax.tasks.process:main',
            'cax-mv = cax.main:move',
            'cax-rm = cax.main:remove',
            'cax-stray = cax.main:stray',
            'cax-status = cax.main:status'
        ],
    },
)
