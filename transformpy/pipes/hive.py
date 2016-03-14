__all__ = ['HiveToDictInput', 'DictToHiveOutput']

import csv

from ..base import TransformPipe, SourcePipe, SinkPipe

class HiveToDictInput(SourcePipe):

    def init(self, fields):
        self.fields = fields

    def apply(self, data):
        return csv.DictReader(data, delimiter='\t', doublequote=False, fieldnames=self.fields)

class DictToHiveOutput(SinkPipe):

    def init(self, fields):
        self.fields = fields

    def apply(self, data):
        for row in data:
            assert isinstance(row, dict), "DictToHiveOutput only parses dictionaries."
            output = [ str(row[k]) for k in self.fields ]
            print( '\t'.join( output ) )
