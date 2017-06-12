from __future__ import absolute_import
import csv
from ..base import TransformPipe, SourcePipe, SinkPipe

__all__ = ['HiveToDictInput', 'DictToHiveOutput']


class HiveToDictInput(SourcePipe):

    def init(self, fields):
        self.fields = []
        self.schema = {}

        def wrap_conv(field, conv):
            def wrapped(x):
                try:
                    return conv(x)
                except Exception as e:
                    raise RuntimeError("Failed to convert field `{}` with value '{}'. Error was: {}".format(field, x, str(e)))
            return wrapped

        for field in fields:
            if isinstance(field, (list, tuple)):
                self.fields.append(field[0])
                self.schema[field[0]] = wrap_conv(field[0], field[1])
            else:
                self.fields.append(field)

    def apply(self, data):
        for row in csv.DictReader(
                data, delimiter='\t', doublequote=False,
                fieldnames=self.fields):
            yield {
                k: None if v == r'\N' else self.schema.get(k, lambda x: x)(v)
                for k, v in row.items()
            }


class DictToHiveOutput(SinkPipe):

    def init(self, fields):
        self.fields = fields

    def apply(self, data):
        for row in data:
            assert isinstance(row, dict), "DictToHiveOutput only parses dictionaries."
            output = [str(row[k]) for k in self.fields]
            print('\t'.join(output))
