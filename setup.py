#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

PROJECT = 'cax'
VERSION = '5.1.0'

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'checksumdir', 'scp', 'pagerduty-api', 'pymongo', 'paramiko',
    'numpy', 'sympy', 'pytz', 
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
    data_files=[ ('cax', ['cax/cax.json']),
                 ('cax/host_config', ['cax/host_config/tegner_bash_p3.config', 'cax/host_config/tegner_bash_p2.config', 'cax/host_config/midway_bash_p3.config', 'cax/host_config/midway_bash_p2.config', 'cax/host_config/xe1tdatamanager_bash_p3.config', 'cax/host_config/xe1tdatamanager_bash_p2.config'])
                ],
    license="ISCL",
    zip_safe=False,
    keywords='cax',
    classifiers=[
        'Intended Audience :: System Administrators',
        'Development Status :: 5 - Production/Stable'
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
            'massive-cax = cax.main:massive',
            'caxer = cax.main:main',  # For uniformity with paxer
            'cax-process = cax.tasks.process:main',
            'cax-mv = cax.main:move',
            'cax-rm = cax.main:remove',
            'cax-stray = cax.main:stray',
            'cax-status = cax.main:status',
            'massive-tsm = cax.main:massive_tsmclient',
            'cax-tsm-remove = cax.main:remove_from_tsm',
            'cax-tsm-watch = cax.main:cax_tape_log_file',
            'ruciax = cax.main:ruciax',
            'ruciax-rm = cax.main:remove_from_rucio',
            'massive-ruciax = cax.main:massiveruciax',
            'ruciax-check = cax.main:ruciax_status',
            'ruciax-purge = cax.main:ruciax_purge',
            'ruciax-download = cax.main:ruciax_download',
            'ruciax-locator = cax.main:ruciax_locator',
        ],
    },
)
