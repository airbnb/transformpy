import os
from setuptools import setup

# Used to read README.md into long description
def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()

setup(
    name = "transformpy",
    version = "0.2",
    author = "Matthew Wardrop",
    author_email = "matthew.wardrop@airbnb.com",
    description = ("A general purpose python ETL/pipeline utility library, for use especially with Hive Streaming."),
    keywords = "pipeline transform hive",
    packages=['transformpy', 'transformpy.pipes'],
    long_description=read('README.md'),
)
