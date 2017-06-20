from itertools import chain

import six

from ..base import TransformPipe, TransformType, SourcePipe, SinkPipe

__all__ = ['SimpleClusteringPipe', 'RangeClusteringPipe']


class SimpleClusteringPipe(TransformPipe):

    def init(self, feature, when=lambda x, y: x == y):
        self.feature = feature
        self.is_peer = when

    def apply(self, data):
        data = iter(data)

        class state:
            current_group = None
            seen_groups = {}
            next_generator = None
            first_row = None

        def get_group(row):
            if isinstance(self.feature, six.string_types):
                return row[self.feature]
            return self.feature(row)

        def cluster_generator(data):
            if state.first_row is not None:
                yield state.first_row
            for row in data:
                if state.current_group is None:
                    state.current_group = get_group(row)
                    state.seen_groups[get_group(row)] = True
                if get_group(row) != state.current_group:
                    assert get_group(row) not in state.seen_groups, "SimpleClusteringPipe assumes that data is pre-ordered such that elements of the same cluster are next to one another. In this case, %s has already been seen." % (get_group(row))
                    state.seen_groups[get_group(row)] = True
                    if state.current_group is not None and not self.is_peer(state.current_group, get_group(row)):
                        state.first_row = row
                        state.current_group = get_group(row)
                        state.next_generator = cluster_generator(data)
                        return
                yield row
            state.next_generator = None

        state.next_generator = cluster_generator(data)

        while state.next_generator is not None:
            ng = state.next_generator
            yield ng

    @property
    def type(self):
        return TransformType.CLUSTER


class RangeClusteringPipe(TransformPipe):

    # TODO: Use assumption that data is sorted by min_key
    # TODO: Use SortedListWithKey from sortedcontainers for clusters array

    class RangeCluster(object):
        def __init__(self, start, end, *rows):
            self.start = start
            self.end = end
            self.rows = list(rows)

        def __repr__(self):
            return "RangeCluster(start=%s, end=%s, count=%d)" % (self.start, self.end, len(self.rows))

        def add(self, start, end, row):
            self.start = min(self.start, start)
            self.end = max(self.end, end)
            self.rows.append(row)

        def merge(self, other):
            self.start = min(self.start, other.start)
            self.end = max(self.end, other.end)
            self.rows.extend(other.rows)

    def init(self, min_field, max_field, cluster_on=None):
        self.min_field = min_field
        self.max_field = max_field
        if cluster_on is None:
            cluster_on = (lambda c_start, c_end, start, end: (start <= c_end) and (end >= c_start))
        self.cluster_on = cluster_on

    def apply(self, data):
        # We need to pool all available data at this step.
        # TODO: Make use of assumption that data is sorted by min key.
        data = list(data)

        last = None

        clusters = []
        for row, entry in enumerate(data):
            # Assert that data is sorted by min key. May be used later to optimise
            # this method.
            if last is not None:
                assert last <= entry[self.min_field], "RangeClusteringPipe requires that data be sorted by min_field. %s=%s was out of order (followed %s)." % (self.min_field, entry[self.min_field], last)
            last = entry[self.min_field]

            self._add_to_clusters(clusters, entry[self.min_field], entry[self.max_field], row)
        for cluster in clusters:
            yield [data[row] for row in cluster.rows]

    def _add_to_clusters(self, clusters, start, end, row):
        indices = []

        # Check to which clusters the new entry belongs
        for i, cluster in enumerate(clusters):
            if self.cluster_on(cluster.start, cluster.end, start, end):
                indices.append(i)

        # If it belongs to no clusters, add a new one
        if len(indices) == 0:
            clusters.append(self.RangeCluster(start, end, row))
            return clusters

        # If it belongs to multiple clusters, merge them, and then add to the resulting cluster
        if len(indices) > 0:
            clusters = self._merge_clusters(clusters, *indices)
        clusters[indices[0]].add(start, end, row)

        return clusters

    def _merge_clusters(self, clusters, *indices):
        indices = sorted(indices)
        i = indices[0]
        merged_cluster = clusters[i]
        for index in indices[:1:-1]:
            merged_cluster.merge(clusters[index])
            del clusters[index]
        return clusters

    @property
    def type(self):
        return TransformType.CLUSTER
