from ..base import TransformPipe, TransformType

__all__ = ['FieldFilterPipe', 'FieldMergePipe']


class FieldFilterPipe(TransformPipe):

    def init(self, fields):
        self.fields = fields

    def apply(self, data):
        for row in data:
            yield {field: row[field] for field in self.fields}

    @property
    def type(self):
        return TransformType.MAP


class FieldMergePipe(TransformPipe):

    def init(self):
        pass

    def apply(self, data):
        for row in data:
            result = {}
            for d in row:
                result.update(d)
            yield result

    @property
    def type(self):
        return TransformType.FANIN
