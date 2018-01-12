#!/usr/bin/env python

from pip.req import parse_requirements
import setuptools

remote_requirements = '\n'.join(str(r.req) for r in parse_requirements("requirements.txt", session='dummy') if r.req)

setuptools.setup(
    name='cp-schema-registry-tests',
    version='4.1.0',

    author="Confluent, Inc.",

    description='Docker image tests',

    url="https://github.com/confluentinc/schema-registry",

    install_requires=remote_requirements,

    packages=['test'],

    include_package_data=True,

    python_requires='>=2.7',
    setup_requires=['setuptools-git'],

)
