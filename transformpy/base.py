from __future__ import print_function

import inspect
from abc import ABCMeta, abstractproperty, abstractmethod

__all__ = ['Transform', 'TransformType', 'TransformPipe', 'SourcePipe', 'SinkPipe',
            'TeePipe', 'FunctionWrapperPipe', 'FunctionWrapperSinkPipe', 'NestedPipe']

class Transform(object):

    def __init__(self):
        self._pipeline = []
        self._sinks = []

    def __ins(self, obj, type, args, kwargs):
        if inspect.isclass(obj):
            obj = obj(*args, **kwargs)
        elif inspect.isfunction(obj):
            if type is not 'output':
                obj = FunctionWrapperPipe(obj, type)
            else:
                obj = FunctionWrapperSinkPipe(obj, type)
        return obj

    def __ins_add(self, list, obj, type, args, kwargs):
        obj = self.__ins(obj, type, args, kwargs)
        assert isinstance(obj, Transform) or obj.type is None or obj.type == type, "Object %s (of type %s) is not of the right type (%s)." % (obj, obj.type, type)
        list.append(obj)
        return self

    def input(self, mapper, *args, **kwargs):
        assert len(self._pipeline) == 0, "Input must be the first transform pipeline to be added."
        return self.__ins_add(self._pipeline, mapper, TransformType.SOURCE, args, kwargs)

    def map(self, mapper, *args, **kwargs):
        return self.__ins_add(self._pipeline, mapper, TransformType.MAP, args, kwargs)

    def cluster(self, clusterer, *args, **kwargs):
        return self.__ins_add(self._pipeline, clusterer, TransformType.CLUSTER, args, kwargs)

    def aggregate(self, reducer, *args, **kwargs):
        return self.__ins_add(self._pipeline, reducer, TransformType.AGGREGATE, args, kwargs)

    def tee(self, tee, *args, **kwargs):
        if not isinstance(tee, TeePipe):
            tee = TeePipe(tee)
        return self.__ins_add(self._pipeline, tee, TransformType.TEE, args, kwargs)

    def nested(self, nested, *args, **kwargs):
        obj = NestedPipe(self.__ins(nested, TransformType.MAP, args, kwargs))
        return self.__ins_add(self._pipeline, obj, TransformType.NESTED, args, kwargs)

    def output(self, outputter, *args, **kwargs):
        return self.__ins_add(self._sinks, outputter, TransformType.SINK, args, kwargs)

    def apply(self, data):
        r = data
        for tp in self._pipeline:
            r = tp.apply(r)

        if len(self._sinks) > 0:
            for output in r:
                for outputter in self._sinks:
                    outputter.apply(output)
        else:
            return r

class TransformType(object):
    SOURCE = 'source'
    SINK = 'sink'
    TEE = 'tee'
    MAP = 'map'
    CLUSTER = 'cluster'
    AGGREGATE = 'aggregate'
    NESTED = 'nested'

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

class SourcePipe(TransformPipe):

    @property
    def type(self):
        return 'source'

class SinkPipe(TransformPipe):

    @property
    def type(self):
        return 'sink'

class NestedPipe(TransformPipe):

    def init(self, pipe):
        self.pipe = pipe

    def apply(self, data):
        for datum in data:
            yield list(self.pipe.apply(datum))

    @property
    def type(self):
        return TransformType.NESTED

class FunctionWrapperPipe(TransformPipe):

    def init(self, function, type=None):
        assert callable(function), "Wrapped objects must be callable."
        self.__function = function
        self._type = type

    def apply(self, data):
        for datum in data:
            yield self.__function(datum)

    @property
    def type(self):
        return self._type

class FunctionWrapperSinkPipe(TransformPipe):

    def init(self, function, type=None):
        assert callable(function), "Wrapped objects must be callable."
        self.__function = function
        self._type = type

    def apply(self, data):
        return self.__function(data)

    @property
    def type(self):
        return self._type

class TeePipe(TransformPipe):

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
        return TransformType.TEE
