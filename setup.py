#!/usr/bin/env python3

import setuptools
from distutils.core import setup

setup(name='pyhttpfs',
      version='1.0',
      description='Python HTTP rest to FUSE bindings',
      author='Paul Jordan',
      author_email='paullj1@gmail.com',
      packages=['pyhttpfs'],
      dependencies=[
          'httpx',
          'pyfuse3',
          'tabulate'
      ],
      entry_points = {
          'console_scripts': [
              'pyhttpfs=pyhttpfs.pyhttpfs:main',
              'pyhttpfs-server=pyhttpfs.server:main',
          ],
      })



