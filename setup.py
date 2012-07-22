# setup.py
# Copyright (C) 2012 Accellion, Inc.
#
# This library is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; version 2.1.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301 USA

import os
import os.path

# Fallback on distutils if setuptools is unavailable -- don't force
# the user to install it.
try:
    from setuptools import setup
    from setuptools import find_packages
except:
    from distutils.core import setup
    from distutils.dist import DistributionMetadata
    DistributionMetadata.extras_require = None
    DistributionMetadata.test_suite = None
    DistributionMetadata.zip_safe = None
    def find_packages(exclude = []):
        root_dir = os.path.dirname(__file__)
        packages = []
        if root_dir == '':
            root_dir = '.'
        for dirpath, dirnames, filenames in os.walk(root_dir):
            # Ignore dirnames that start with '.'
            for i, dirname in enumerate(dirnames):
                if dirname.startswith('.'): del dirnames[i]
            if '__init__.py' in filenames:
                split_path = []
                while dirpath:
                    head, tail = os.path.split(dirpath)
                    if head == dirpath:
                        break # ??
                    split_path.append(tail)
                    dirpath = head
                split_path.reverse()
                pkg_name = '.'.join(split_path)
                if pkg_name not in exclude:
                    packages.append(pkg_name)

# TODO: set development version numbers with git-describe

with open('README') as readme:
    long_description = readme.read()

setup(
    name             = 'Gobpersist',
    version          = '0.1dev',
    description      = 'Generic OBject Persistence---an ORM-like interface to NoSQL (or SQL)',
    long_description = long_description,
    author           = 'Accellion, Inc.',
    author_email     = 'opensource@accellion.com',
    maintainer       = 'Evan Buswell',
    maintainer_email = 'evan.buswell@accellion.com',
    url              = 'http://accellion.github.com/gobpersist',
    download_url     = 'http://github.com/accellion/gobpersist/downloads',
    keywords         = ['database', 'NoSQL', 'ORM', 'O/RM', 'gobpersist', 'gob'],
    requires         = ['iso8601.py'],
    extras_require   = { # only makes sense to setuputils/distribute
        'memcached': ['pylibmc'],
        'tokyo tyrant': ['pytyrant']
    },
    classifiers      = [
        'Development Status :: 3 - Alpha',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'Intended Audience :: Other Audience',
        'License :: OSI Approved',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'License :: OSI Approved :: GNU Lesser General Public License v2 (LGPLv2)',
        'Operating System :: OS Independent',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Database',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Clustering',
        'Topic :: System :: Distributed Computing'
    ],
    test_suite       = 'gobpersist.test',
    packages         = find_packages(exclude=[]),
    zip_safe         = True # only makes sense to setuputils/distribute
)
