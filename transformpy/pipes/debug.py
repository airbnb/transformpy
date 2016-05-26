from __future__ import print_function
import sys
from ..base import TransformPipe, SinkPipe

__all__ = ['DebugSink']


class DebugSink(SinkPipe):

    def init(self, file=None):
        self.output_file = file
        if isinstance(self.output_file, str):
            self.output_file = open(self.output_file, 'w')

    def apply(self, data):
        for row in data:
            if self.output_file is None:
                try:
                    import termcolor
                    row = termcolor.colored(row, 'yellow', attrs=['bold'])
                except:
                    pass
            print(row, file=self.output_file or sys.stderr)
