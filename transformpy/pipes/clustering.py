__all__ = ['SimpleClusteringPipe']

from ..base import TransformPipe, TransformType, SourcePipe, SinkPipe

class SimpleClusteringPipe(TransformPipe):

    def init(self, field):
        self.field = field
        self.seen_groups = {}

    def apply(self, data):
        cur_group = None
        cur_data = []
        for row in data:
            if row[self.field] != cur_group:
                if cur_group is not None:
                    yield cur_data
                cur_data = []
                cur_group = row[self.field]
                assert cur_group not in self.seen_groups, "SimpleClusterPipe assumes that data is sorted by key. %s=%s was out of order." % (self.field, cur_group)
                self.seen_groups[cur_group] = True
            cur_data.append(row)
        yield cur_data

    @property
    def type(self):
        return TransformType.CLUSTER
