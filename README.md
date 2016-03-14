# TransformPy #

`transformpy` is a Python 2 module for doing transforms on "streams" of data.
The transforms can be applied to any python iterable object, and so can be used
for continuous real_time streams or static streams (such as from a file). It
is designed in such a manner that it uses very little memory (unless necessary
by clustering and/or aggregation routines). It was originally designed to
allow python transformations (maps and reductions) of data stored within HIVE,
using the Hadoop streaming paradigm.

**NOTE:** TransformPy is *not* guaranteed to be API stable before version 1.0;
but changes should be small if any to the current version.

## Installing TransformPy ##

Run inside of a checked out transformpy repository:
```
$ pip install --user .
```

## Using TransformPy ##

Since all of what TransformPy does can be done manually without too much effort,
its entire purpose is to make it simpler to get things done quickly; and to then
debug one's work. The entire TransformPy workflow consists of making "pipelines".

To create a new pipeline one simply creates a new `Transform` object, and then
adds as many pipes to the pipeline as you desire. For example, a simple no-op
pipeline might look like (where we assume that this is being run in a script
and the iterable source is `sys.stdin`):

```
Transform()\
    .input(HiveToDictInput, fields=['id', 'name', 'pet'])\
    .output(DictToHiveOutput, fields=['id', 'name', 'pet'])\
    .apply(sys.stdin)
```

In the above example, two pipes have been used: HiveToDictInput and
DictToHiveOutput; with types of `TransformType.SOURCE` and `TransformType.SINK`
respectively. There are currently eight different types of pipes (though the differences
are largely semantic):
 - `TransformType.SOURCE`: There can be at most one input pipe per pipeline, though it is just a `TransformType.MAP` by another name.
 - `TransformType.SINK`: The sink is the output. A pipeline can have arbitrary many outputs, but they will always be called at the end of the pipeline. To use an output sink in the middle of a pipeline (for debug or verification purposes, use `TransformType.TEE`). If no sinks are provided in the pipeline, a generator object is returned; allowing one to chain the transform.
 - `TransformType.MAP`: This allows one to perform any mapping of input data. By convention, input and output of the pipe should be bijective.
 - `TransformType.CLUSTER`: This allows one to perform clustering of input data. By convention, output should be an iterable containing (perhaps modified) input rows.
 - `TransformType.AGGREGATE`: This allows one to aggregate over clusters, in any way that seems fit. Output from the pipe should be single instances (they de-cluster clusters with whatever aggregation is appropriate).
 - `TransformType.TEE`: This is a no-op pipe that passes the contents of the pipe to a `Transform.SINK` object as well as passing the contents forward into the pipeline.
 - `TransformType.NESTED` : Applies a specified `TransformPipe` or `Transform` object to data one level nested in the input stream. This is useful when performing sub-transforms on clusters.
 - `TransformType.FANOUT` : Forks the pipe into arbitrarily many subpipes, which is useful if there are different pipelines to run on subsets of input data.
 - `TransformType.FANIN` : Merges multiple pipeline results into one. This is similar to aggregation, but more about merging results than summing over rows.

These types are respective added to the pipeline using the following methods of
`Transform`:
 - `input` for `TransformType.SOURCE`
 - `output` for `TransformType.SINK`
 - `map` for `TransformType.MAP`
 - `cluster` for `TransformType.CLUSTER`
 - `aggregate` for `TransformType.AGGREGATE`
 - `tee` for `TransformType.TEE`
 - `nested` for `TransformType.NESTED`
 - `fanout` for `TransformType.FANOUT`
 - `fanin` for `TransformType.FANIN`

All of these methods accept a function object (in which case they are
wrapped by a `FunctionWrapperPipe` object), a class instance of the specified type,
or a class name, followed by instantiation arguments. For example, all of the following
are valid (and equivalent):
 - `Transform().map(lambda x: x['test'])`
 - `Transfrom().map(SimpleMapper, 'test')` [assuming SimpleMapper is defined appropriately]
 - `Transform().map(SimpleMapper('test'))`
 - `Transform().map(simple_mapper_instance)`

There are a couple of special methods on `Transform` objects, including:
 - `unnest` : Which suppresses one level of nesting (using an `UnnestPipe`).
 - `apply` : Which applies the transform pipeline to an iterable dataset.

## Pipes defined by TransformPy ##
** Please note, these will grow over time; small though in number they be now.
Please do contribute your pipes if you think they are useful. **

### transformpy[.base] ###
The pipes in this section are specified for completeness. It will not in general be necessary for you to manually interact with them (since they are automatically used as necessary by `Transform` instances).
 - `FunctionWrapperPipe` : Wraps around functions to integrate them into the TransformPy pipeline as a pipe.
 - `FunctionWrapperSinkPipe` : Wraps around functions to integrate them into the TransformPy pipeline as a sink.
 - `TeePipe` : Wraps around functions and `SinkPipe` subclasses to inspect the contents of a pipe without disturbing it. **This is very useful for debugging! Use as `Transform.tee`.**
 - `NestedPipe` : This applies a `TransformPipe` or `Transform` object to data one level deeper. This is useful when using clustering.
 - `UnnestPipe` : Suppresses one level of nesting. Useful if clustering/aggregation requires manual suppression of nesting.

### transformpy.pipes.hive ###
 - `HiveToDictInput` : reads in tab separated csv files in the HIVE format, and outputs a dictionary object per row. Fields are named according to the passed `fields` parameter.
 - `DictToHiveOutput` : writes dictionary objects (or lists of dictionary objects) to standard out as a tab separated csv of the HIVE format. The fields (and order of those fields) are specfied according to the passed `fields` parameter.

### transformpy.pipes.clustering ###
 - `SimpleClusteringPipe` : clusters rows with the same key into a list of rows. It assumes that rows are sorted by the appropriate key. Clusters can then be acted upon by maps and/or aggregations. To remove nesting, use the `Transform.flatten` method.

### transformpy.pipe.field ###
 - `FieldFilterPipe` : maps dictionaries having many keys to dictionaries having only the keys specified. The keys *must* be present in the original dictionary.
 - `FieldMergePipe` : a fanin pipe that merges dictionaries. Dictionaries are assumed to hold no conflicting information. In the event hat conflicting information is present, the values in dictionaries with higher index are chosen.

## Defining Custom Pipes ##
The chances are you will want (and need) to define your own logic. Doing so is very easy in TransformPy. You can either define a function, and refer to it in the pipeline, or define a class (which will allow you to be smarter with memory, etc.). To use a function, simply pass it to the appropriate method of `Transform`, as described above.

To create an input pipe, simply subclass `transformpy.SourcePipe`. You will need to specify at least two methods: `init` and `apply`. It is important that `apply` returns an iterable object. Make use of `yield` if your methods do not give rise naturally to such an object.

To create a map, cluster, or aggregate pipe, simply subclass `transformpy.TransformPipe`. As above, you must implement `init` and `apply`, but also `type` (as a property), which should return a `TransformType` property.

To create an output pipe/sink, simply subclass `transformpy.SinkPipe`. You will need to specify at least two methods: `init` and `apply`. Apply need not return anything, but it must not return an iterator (since the output will not be inspected).

For example, look at the implementation of `SimpleClusterPipe`:
```
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
```
