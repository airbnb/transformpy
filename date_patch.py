class DateClusterer(AbstractClusterer):

    __shortname__ = 'date'

    def init(self, fields=None):
        if isinstance(fields, str):
            fields = fields.split(',')
        assert len(fields) in (3,4)
        grouped = len(fields) == 4

        if grouped:
            self.grouped = True
            self.id, self.group, self.start, self.end = fields
        else:
            self.grouped = False
            self.id, self.start, self.end = fields

    def reset(self):
        pass

    def merge_clusters(self, clusters, *indices):
        indices = sorted(indices)
        i = indices[0]
        dout = clusters[i]
        for index in indices[:1:-1]:
            dout.merge(clusters[index])
            del clusters[index]
        return clusters

    def add_to_clusters(self, clusters, id, start, end, *rows):
        indices = []

        for i,date_cluster in enumerate(clusters):
            if date_cluster.should_include(start, end):
                indices.append(i)

        if len(indices) == 0:
            clusters.append(DateCluster(id, start, end, *rows))
            return clusters

        if len(indices) > 0:
            clusters = self.merge_clusters(clusters, *indices)
        clusters[indices[0]].add(*rows)

        return clusters

    def cluster(self, data, agg):
        cur_group = None
        cur_index = 0
        cur_data = []
        clusters = []
        r = None
        for row, entry in enumerate(data):
            if self.grouped and entry[self.group] != cur_group:
                if cur_group != None:
                    r = self.cluster_agg(agg, cur_data, clusters, cur_group, results=r)
                    clusters = []
                    cur_data = []
                    cur_index = 0
                cur_group = entry[self.group]
            cur_data.append(entry)
            self.add_to_clusters(clusters, entry[self.id], entry[self.start], entry[self.end], cur_index)
            cur_index += 1
        return self.cluster_agg(agg, cur_data, clusters, cur_group, results=r)

    def cluster_agg(self, agg, data, clusters, group, results=None):
        if group in self.groups_handled:
            raise ValueError("Group already processed for %s." % group)
        for cluster in clusters:
            results = agg(data, cluster, results=results)
        self.groups_handled[group] = True
        return results

class DateCluster(object):

    def __init__(self, id, date_start, date_end, *rows):
        self.id = id
        self.date_start = date_start
        self.date_end = date_end
        self.rows = list(rows)

    def __repr__(self):
        return "DateCluster(%s, %s, %d)" % (self.date_start, self.date_end, len(self.rows))

    def merge(self, other):
        self.date_start = min(self.date_start, other.date_start)
        self.date_end = min(self.date_end, other.date_end)
        self.rows.extend(other.rows)

    def should_include(self, start, end):
        return (start <= self.date_end) and (end >= self.date_start)

    def add(self, *rows):
        self.rows.extend(list(rows))

    @property
    def props(self):
        return {'id_cluster': self.id, 'cluster_start': self.date_start, 'cluster_end': self.date_end}
