from __future__ import absolute_import
from builtins import str
import pandas
from ..base import TransformPipe, SourcePipe, SinkPipe

__all__ = ['PandasToDictInput', 'DictToPandasOutput']


class PandasToDictInput(SourcePipe):

    def init(self, fields):
        self.fields = fields

    def apply(self, data):
        assert isinstance(data, pandas.DataFrame)
        for i, row in data.iterrows():
            rowd = dict(row)
            yield {field: rowd[field] for field in self.fields}


class DictToPandasOutput(SinkPipe):

    def init(self, fields):
        self.fields = fields
        self.rows = []

    def apply(self, data):
        for row in data:
            self.rows.append({field: row[field] for field in self.fields})

    @property
    def df(self):
        return pandas.DataFrame(self.rows)

    def reset(self):
        self.rows = []
