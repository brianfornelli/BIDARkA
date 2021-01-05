#!/usr/bin/env python
import io
import os

from setuptools import find_packages, setup

# Package meta-data.
NAME = 'bidarka'
DESCRIPTION = 'BIg Data ARchitecture Abstraction tool'
URL = 'unknown'
EMAIL = 'brianfornelli@gmail.com'
AUTHOR = 'brianfornelli@gmail.com'
REQUIRES_PYTHON = '>=3.6'
#from bidarka import __version__ as VERSION
VERSION = None

REQUIRED = ['pyspark>=0.2.3', 'numpy', 'pandas']

here = os.path.abspath(os.path.dirname(__file__))

# Import the README and use it as the long-description.
# Note: this will only work if 'README.md' is present in your MANIFEST.in file!
with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = '\n' + f.read()

# Load the package's __version__.py module as a dictionary.
about = {}
if not VERSION:
    with open(os.path.join(here, NAME, '__version__.py')) as f:
        exec (f.read(), about)
else:
    about['__version__'] = VERSION

#with open('requirements.txt') as f:
    #REQUIRED = f.read().splitlines()

setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=('tests',)),
    install_requires=REQUIRED,
    include_package_data=True,
    license='Proprietary',
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'License :: Other/Proprietary License',
        'Operating System :: POSIX :: Linux'
    ],
    entry_points = {
        'console_scripts': [
            'bidarka = bidarka._cmdline:main'
        ]
    }
)
