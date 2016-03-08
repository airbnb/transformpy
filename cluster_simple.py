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
        self.groups_handled = []
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
            data[row]['id_cluster'] = cluster.id
            output = [ str(data[row][k]) for k in self.fields ]
            print( '\t'.join( output ) )

class DateClusterer(AbstractClusterer):

    __shortname__ = 'date'

    def init(self, fields=None):
        if isinstance(fields, str):
            fields = fields.split(',')
        assert len(fields) in (3,4)
        grouped = len(fields) == 4

        if grouped:
            self.grouped = True
            self.id, self.group, self.start, self.end = fields
        else:
            self.grouped = False
            self.id, self.start, self.end = fields

    def reset(self):
        pass

    def merge_clusters(self, clusters, *indices):
        indices = sorted(indices)
        i = indices[0]
        dout = clusters[i]
        for index in indices[:1:-1]:
            dout.merge(clusters[index])
            del clusters[index]
        return clusters

    def add_to_clusters(self, clusters, id, start, end, *rows):
        indices = []

        for i,date_cluster in enumerate(clusters):
            if date_cluster.should_include(start, end):
                indices.append(i)

        if len(indices) == 0:
            clusters.append(DateCluster(id, start, end, *rows))
            return clusters

        if len(indices) > 0:
            clusters = self.merge_clusters(clusters, *indices)
        clusters[indices[0]].add(*rows)

        return clusters

    def cluster(self, data, agg):
        cur_group = None
        cur_index = 0
        cur_data = []
        clusters = []
        r = None
        for row, entry in enumerate(data):
            if self.grouped and entry[self.group] != cur_group:
                if cur_group != None:
                    r = self.cluster_agg(agg, cur_data, clusters, cur_group, results=r)
                    clusters = []
                    cur_data = []
                    cur_index = 0
                cur_group = entry[self.group]
            cur_data.append(entry)
            self.add_to_clusters(clusters, entry[self.id], entry[self.start], entry[self.end], cur_index)
            cur_index += 1
        return self.cluster_agg(agg, cur_data, clusters, cur_group, results=r)

    def cluster_agg(self, agg, data, clusters, group, results=None):
        if group in self.groups_handled:
            raise ValueError("Group already processed for %s." % group)
        for cluster in clusters:
            results = agg(data, cluster, results=results)
        self.groups_handled.append(group)
        return results

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

class DateCluster(object):

    def __init__(self, id, date_start, date_end, *rows):
        self.id = id
        self.date_start = date_start
        self.date_end = date_end
        self.rows = list(rows)

    def __repr__(self):
        return "DateCluster(%s, %s, %d)" % (self.date_start, self.date_end, len(self.rows))

    def merge(self, other):
        self.date_start = min(self.date_start, other.date_start)
        self.date_end = min(self.date_end, other.date_end)
        self.rows.extend(other.rows)

    def should_include(self, start, end):
        return (start <= self.date_end) and (end >= self.date_start)

    def add(self, *rows):
        self.rows.extend(list(rows))

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
        output_fields = args.fields.split(',')
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
