import os

from setuptools import setup

description = '''
`transformpy` is a Python 2/3 module for doing transforms on "streams" of data.
The transforms can be applied to any python iterable object, and so can be used
for continuous real_time streams or static streams (such as from a file). It
is designed in such a manner that it uses very little memory (unless necessary
by clustering and/or aggregation routines). It was originally designed to
allow python transformations (maps and reductions) of data stored within HIVE,
using the Hadoop streaming paradigm.

**NOTE:** TransformPy is *not* guaranteed to be API stable before version 1.0;
but changes should be small if any to the current version.
'''

setup(
    name="transformpy",
    version="0.3.2",
    author="Matthew Wardrop",
    author_email="matthew.wardrop@airbnb.com",
    description="A general purpose python ETL/pipeline utility library, for use especially with Hive Streaming.",
    keywords="pipeline transform hive",
    packages=['transformpy', 'transformpy.pipes'],
    install_requires=[
        'future',
        'pandas'
    ],
    long_description=description,
)
