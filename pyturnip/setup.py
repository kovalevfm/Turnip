#!/usr/bin/python
# coding: utf-8
from distutils.core import setup

setup(
    name='turnip',
    version='1.0',
    description='key value server , store data on disk with leveldv, server on zmq, protocol via msgpack',
    author='Fedor Kovalev',
    author_email='kovalevfm@yandex-team.ru',
    py_modules=['turnip', ],
)
