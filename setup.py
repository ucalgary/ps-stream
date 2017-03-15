#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pkg_resources
from setuptools import find_packages
from setuptools import setup


install_requires = [
    'docopt==0.6.2',
    'Twisted==16.6.0',
    'PyYAML==3.12',
    'pytz==2016.10',
    'confluent-kafka[avro]==0.9.4',
    'confluent-schema-registry-client==1.1.0',
    'ujson==1.35'
]


setup(
    name='ps_stream',
    description='Process PeopleSoft sync messages into logical streams',
    author='King Chung Huang',
    packages=find_packages(),
    package_data={
        '': ['*.yml']
    },
    install_requires=install_requires,
    entry_points="""
    [console_scripts]
    ps-stream=ps_stream.cli.main:main
    """,
    zip_safe=True
)
