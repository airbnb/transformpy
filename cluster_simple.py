from __future__ import print_function
'''
INPUT: four tab delimited columns: id_column , start, end[, group_by]
OPT1 : id_cluster, id_column
'''

from abc import ABCMeta, abstractproperty, abstractmethod
import argparse
import inspect

import os, sys, csv

import numpy as np
import pandas as pd

class AbstractClusterer(object):

    __metaclass__ = ABCMeta

    def __init__(self, **kwargs):
        self.groups_handled = {}
        self.init(**kwargs)
        self.reset()

    @abstractmethod
    def init(self, **kwargs):
        pass

    @abstractmethod
    def reset(self):
        pass

    def __call__(self, data):
        return self.cluster(data)

    @abstractmethod
    def cluster(self, data):
        pass

    @classmethod
    def setup_parsers(cls, subparsers):
        parser = subparsers.add_parser(cls.__shortname__)
        parser.set_defaults(which=cls.__shortname__)
        for arg in inspect.getargspec(cls.init).args[1:]:
            parser.add_argument(cls.__shortname__+"_"+arg, action="store")

class AbstractCluster(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def merge(self, other):
        pass

    @abstractmethod
    def should_include(self, datum):
        pass

    @abstractmethod
    def add(self, *ids):
        pass

    @abstractproperty
    def props(self):
        pass

class AbstractAggregator(object):

    __metaclass__ = ABCMeta

    def __init__(self, fields):
        self.fields = fields

    def __call__(self, *args, **kwargs):
        return self.aggregate(*args, **kwargs)

    @abstractmethod
    def aggregate(self, data, cluster, results=None):
        pass

class PrintAggregator(AbstractAggregator):

    __shortname__ = 'print'

    def aggregate(self, data, cluster, results=None):

        for row in cluster.rows:
            data[row].update(cluster.props)
            output = [ str(data[row][k]) for k in self.fields ]
            print( '\t'.join( output ) )

if __name__ == "__main__":

    def extract_dict(d, prefix):
        nd = {}
        for key in d:
            if key.startswith(prefix):
                nd[key[len(prefix):]] = d[key]
        return nd

    parser = argparse.ArgumentParser(description='Hadoop streaming date clusterer.')
    parser.add_argument('fields', action='store', default=None)
    parser.add_argument('--out', dest='output_fields', action='store', default=None)
    parser.add_argument('--patch', action='store', default=None)
    parser.add_argument('--agg', action='store', default=None)

    args, _ = parser.parse_known_args()
    if args.patch is not None:
        exec(open(args.patch))

    subparsers = parser.add_subparsers(help='sub-command help')
    for clusterClass in AbstractClusterer.__subclasses__():
        clusterClass.setup_parsers(subparsers)

    args = parser.parse_args()

    input_fields = args.fields.split(',')
    assert 'id_cluster' not in input_fields

    if args.output_fields is not None:
        output_fields = args.output_fields.split(',')
    else:
        output_fields = ['id_cluster'] + input_fields

    if args.agg is None:
        agg = PrintAggregator(fields=output_fields)
    else:
        for aggClass in AbstractAggregator.__subclasses__():
            if aggClass.__shortname__ == args.agg:
                agg = aggClass(fields=output_fields)

    csv_reader = csv.DictReader(sys.stdin, delimiter='\t', doublequote=False, fieldnames=input_fields)

    for clusterClass in AbstractClusterer.__subclasses__():
        if clusterClass.__shortname__ == args.which:
            clusterer = clusterClass(**extract_dict(vars(args),clusterClass.__shortname__+'_'))
            clusterer.cluster(csv_reader, agg=agg)
            break
