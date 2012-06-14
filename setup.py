from distutils.core import setup
from setuptools import find_packages
import sys, os


Name = 'gobpersist'
Version = '0.1'
Summary = 'Gobpersist Package'
Description = 'SyncSat Gobpersist Library'
Keywords = 'gobpersist syncsat'
Author = 'An Doan'
AuthorEmail = 'an.doan@accellion.com'
ProjectUrl = ''
License = 'Test'
Packages = find_packages(exclude=['ez_setup', 'examples', 'tests'])
ExcludePackageData = {'':['TODO', 'sample.txt', 'test.py']}
Classifiers= [
      'Development Status :: 1 - Planning',
      'Environment :: Other Environment',
      'Intended Audience :: Developers',
      'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
      'Operating System :: OS Independent',
      'Programming Language :: Python :: 2.7',
]
InstallRequires=[
]
EntryPoints={
}


setup(name=Name,
      version=Version,
      description=Summary,
      long_description=Description,
      classifiers=Classifiers,
      keywords=Keywords,
      author=Author,
      author_email=AuthorEmail,
      url=ProjectUrl,
      license=License,
      packages=Packages,
      include_package_data=True,
      #exclude_package_data=ExcludePackageData, # this does not work
      #exclude_package_data={'':['tests/*']}, # this works if 'test.py' is put under subdir 'tests'
      zip_safe=True,
      install_requires=InstallRequires,
      entry_points=EntryPoints,
      )
