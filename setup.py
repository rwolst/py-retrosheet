#!/usr/bin/env python

from distutils.core import setup
import codecs
import sys
import os
import os.path as path
import shutil


# where this file is located
cwd = path.dirname(__file__)

# setup options
setup(
    name='py-retrosheet',
    license='MIT',
    description='Tools for building a Retrosheet database.',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Other Audience',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Natural Language :: English',
        'Topic :: Other/Nonlisted Topic',
    ],
    keywords=[
        'MLB',
        'baseball',
        'Major League Baseball',
        'baseball scores',
        'baseball stats',
        'baseball data',
        'MLB GameDay',
    ],
    platforms='ANY',
    install_requires=['sqlalchemy', 'psycopg2', 'click', 'wget', 'pandas'],
    packages=['pyretro', 'pyretro/classes', 'pyretro/cli'],
    data_files=[('./conf', ['pyretro/conf/pyretro_config.ini.dist'])],
    entry_points="""
        [console_scripts]
        pyretro_download=pyretro.cli.download:cli
        pyretro_ensure=pyretro.cli.ensure:cli
        pyretro_parse=pyretro.cli.parse:cli
        pyretro_config=pyretro.cli.config:cli
      """,
)
