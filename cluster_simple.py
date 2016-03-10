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

#from hive_transforms import AbstractClusterer, AbstractAggregator, AbstractCluster, Transformer

# t = Transform().map(IdentityMapper).cluster(NullClusterer).reduce(PrintAggregator).output(PrintOutputter)
# t.apply(data_iterable)

class Transform(object):

    def __init__(self):
        self._pipeline = []
        self._outputters = []

    def __ins_add(self, list, obj, type, args, kwargs):
        if inspect.isclass(obj):
            obj = obj(*args, **kwargs)
        elif inspect.isfunction(obj):
            if type is not 'output':
                obj = FunctionWrapperTransformPipe(obj, type)
            else:
                obj = FunctionWrapperTransformOutput(obj, type)
        assert obj.type is None or obj.type == type, "Object %s is not of the right type." % obj
        list.append(obj)
        return self

    def input(self, mapper, *args, **kwargs):
        assert len(self._pipeline) == 0, "Input must be the first transform pipeline to be added."
        return self.__ins_add(self._pipeline, mapper, 'input', args, kwargs)

    def map(self, mapper, *args, **kwargs):
        return self.__ins_add(self._pipeline, mapper, 'map', args, kwargs)

    def cluster(self, clusterer, *args, **kwargs):
        return self.__ins_add(self._pipeline, clusterer, 'cluster', args, kwargs)

    def aggregate(self, reducer, *args, **kwargs):
        return self.__ins_add(self._pipeline, reducer, 'agg', args, kwargs)

    def tee(self, tee, *args, **kwargs):
        if not isinstance(tee, TeeTransformPipe):
            tee = TeeTransformPipe(tee)
        return self.__ins_add(self._pipeline, tee, 'tee', args, kwargs)

    def output(self, outputter, *args, **kwargs):
        return self.__ins_add(self._outputters, outputter, 'output', args, kwargs)

    def apply(self, data):
        assert len(self._outputters) > 0, "This transform does not output anything!"

        r = data
        for tp in self._pipeline:
            r = tp.apply(r)

        for output in r:
            for outputter in self._outputters:
                outputter.apply(output)

class TransformPipe(object):
    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        self.init(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        for result in self.apply(*args, **kwargs):
            yield result

    @abstractmethod
    def init(self, *args, **kwargs):
        pass

    @abstractmethod
    def apply(self, data):
        pass

    @abstractproperty
    def type(self):
        pass

class FunctionWrapperTransformPipe(TransformPipe):

    def init(self, function, type=None):
        assert callable(function), "Wrapped objects must be callable."
        self.__function = function
        self._type = type

    def apply(self, data):
        for result in self.__function(data):
            yield result

    @property
    def type(self):
        return self._type

class FunctionWrapperTransformOutput(TransformPipe):

    def init(self, function, type=None):
        assert callable(function), "Wrapped objects must be callable."
        self.__function = function
        self._type = type

    def apply(self, data):
        return self.__function(data)

    @property
    def type(self):
        return self._type

class TeeTransformPipe(TransformPipe):

    def init(self, outputter):
        self.is_function = inspect.isfunction(outputter)
        if not self.is_function:
            assert outputter.type == 'output'
        self.__outputter = outputter

    def apply(self, data):
        for row in data:
            if self.is_function:
                self.__outputter(row)
            else:
                self.__outputter.apply(row)
            yield row

    @property
    def type(self):
        return 'tee'

class HiveToDictInput(TransformPipe):

    def init(self, fields):
        self.fields = fields

    def apply(self, data):
        return csv.DictReader(data, delimiter='\t', doublequote=False, fieldnames=self.fields)

    @property
    def type(self):
        return 'input'

class DictToHiveOutput(TransformPipe):

    def init(self, fields):
        self.fields = fields

    def apply(self, row):
        if type(row) == list:
            for r in row:
                self.apply(r)
        else:
            output = [ str(row[k]) for k in self.fields ]
            print( '\t'.join( output ) )

    @property
    def type(self):
        return 'output'

################################################################################

class DateClusterer(TransformPipe):

    def init(self, fields=None):
        self.groups_handled = {}
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

    def apply(self, data):
        cur_group = None
        cur_index = 0
        cur_data = []
        clusters = []
        r = None
        for row, entry in enumerate(data):
            if self.grouped and entry[self.group] != cur_group:
                if cur_group != None:
                    for cluster in self.yield_clusters(cur_data, clusters, cur_group):
                        yield cluster
                    clusters = []
                    cur_data = []
                    cur_index = 0
                cur_group = entry[self.group]
            cur_data.append(entry)
            self.add_to_clusters(clusters, entry[self.id], entry[self.start], entry[self.end], cur_index)
            cur_index += 1
        for cluster in self.yield_clusters(cur_data, clusters, cur_group):
            yield cluster

    def yield_clusters(self, data, clusters, group):
        if group in self.groups_handled:
            raise ValueError("Group already processed for %s." % group)
        for cluster in clusters:
            c_dicts = []
            for row in cluster.rows:
                c_dict = data[row]
                c_dict.update(cluster.props)
                c_dicts.append(c_dict)
            yield c_dicts
        self.groups_handled[group] = True

    @property
    def type(self):
        return 'cluster'

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

    @property
    def props(self):
        return {'id_cluster': self.id, 'cluster_start': self.date_start, 'cluster_end': self.date_end}

def debug(x):
    print(x, file=sys.stderr)

t = Transform()\
    .input(HiveToDictInput, fields=['id','start','end','group'])\
    .cluster(DateClusterer, fields=['id','group','start','end'])\
    .output(DictToHiveOutput, fields=['id_cluster','id'])\
    .output(debug)\
    .apply(sys.stdin)
