#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pkg_resources
from setuptools import find_packages
from setuptools import setup


install_requires = [
	'docopt >= 0.6.2',
	'Twisted >= 16.6.0'
]


setup(
	name='pssync',
	description='Process PeopleSoft sync messages into Kafka topics',
	author='King Chung Huang',
	packages=find_packages(),
	install_requires=install_requires,
	entry_points="""
	[console_scripts]
	pssync=pssync.cli.main:main
	"""
)
