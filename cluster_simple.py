from __future__ import print_function
'''
INPUT: four tab delimited columns: id_column, start, end[, group_by]
OPT1 : id_cluster, id_column
'''

import argparse

import os, sys, csv
from subprocess import Popen, PIPE, STDOUT

import numpy as np
import pandas as pd

from collections import Counter
import ast
import json

class DateCluster(object):

    def __init__(self, date_start, date_end, *rows):
        self.date_start = date_start
        self.date_end = date_end
        self.rows = list(rows)

    def merge(self, other):
        self.date_start = min(self.date_start, other.date_start)
        self.date_end = min(self.date_end, other.date_end)
        self.rows.extend(other.rows)

    def overlaps(self, start, end):
        return (start <= self.date_end) and (end >= self.date_start)

    def add(self, *rows):
        self.rows.extend(list(rows))

def merge_clusters(clusters, *indices):
    indices = sorted(indices)
    i = indices[0]
    dout = clusters[i]
    for index in indices[:1:-1]:
        dout.merge(clusters[index])
        del clusters[index]
    return clusters

def add_to_clusters(clusters, start, end, *ids):
    indices = []
    for i,date_cluster in enumerate(clusters):
        if date_cluster.overlaps(start, end):
            indices.append(i)

    if len(indices) == 0:
        clusters.append(DateCluster(start, end, *ids))
        return clusters

    if len(indices) > 0:
        clusters = merge_clusters(clusters, *indices)
    clusters[indices[0]].add(*ids)

    return clusters

def cluster_rows(df):
    clusters = []
    for row, entry in df.iterrows():
        add_to_clusters(clusters, entry[1], entry[2], entry[0])
    return pd.DataFrame([(cluster.rows[0], row, 'test') for cluster in clusters for row in cluster.rows])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Hadoop streaming date clusterer.')
    parser.add_argument('--grouped', dest='grouped', action='store_const',
                       const=True, default=False, help='specifies whether incoming data is grouped')
    args = parser.parse_args()

    df = pd.read_table(
        sys.stdin, # Input
        sep='\t',
        header=None,
        error_bad_lines=True,
        warn_bad_lines=True
    )

    if args.grouped:
        clustered = df.groupby(3,axis=0).apply(cluster_rows).reset_index(drop=True)
    else:
        clustered = df.apply(cluster_rows)

    clustered.to_csv(sys.stdout, sep="\t", index=False, header=None, quoting=csv.QUOTE_NONE)
