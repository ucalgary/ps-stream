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
    'confluent-kafka==0.11.5',
    'ujson==1.35',
    'docopt_utils==0.0.0'
]

dependency_links = [
    'https://github.com/ucalgary/docopt-utils/archive/master.zip#egg=docopt_utils-0.0.0',
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
    dependency_links=dependency_links,
    entry_points="""
    [console_scripts]
    ps-stream=ps_stream.cli.main:main
    """,
    zip_safe=True
)
