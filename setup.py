"""
XtraBufr
~~~~~~~~~
Extra functions and tools to work with BUFR files. (Depends on ECMWF ecCodes)
"""
import os
from setuptools import setup
import fastentrypoints


def get(x):
    with open(os.path.join(os.path.dirname(__file__),
                           'xtrabufr', '__init__.py')) as f:
        for line in f.readlines():
            if line.startswith('__' + x + '__'):
                return line.split('=')[1].strip()[1:-1]


def get_requirements():
    requirements = ['numpy']
    return requirements


setup(
    name='XtraBufr',
    version=get('version'),
    platforms=['linux', 'darwin'],
    packages=['xtrabufr'],
    package_dir={'xtrabufr': 'xtrabufr'},
    include_package_data=True,
    setup_requires=['pytest-runner'],
    install_requires=get_requirements(),
    tests_require=['pytest'],
    scripts=['bin/xbsort', 'bin/xbsplit', 'bin/xbcp2bin'],
    entry_points={
        'console_scripts': ['xbdef = xtrabufr._scripts_:_xbdef_',
                            'xbcopy = xtrabufr._scripts_:_xbcopy_',
                            'xbprint = xtrabufr._scripts_:_xbprint_',
                            'xbfilter = xtrabufr._scripts_:_xbfilter_',
                            'xbsynop = xtrabufr._scripts_:_xbsynop_'],
    },
    author=get('author'),
    author_email=get('email'),
    description='Extra BUFR tools (build on ecCodes) to work with BUFR files.',
    long_description=__doc__,
    license=get('license'),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: OS dependent",
        "License :: OSI Approved :: AGPL v3.0 License",
        "Programming Language :: Python :: 2.7",
        "Topic :: Utilities",
    ],
    keywords=['BUFR', 'WMO', 'ecCodes'],
    url='https://github.com/isezen/xtrabufr',
)
