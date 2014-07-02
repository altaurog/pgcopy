from datetime import datetime, timedelta
import hashlib
import math
from pgcopy import CopyManager
from . import base

class Benchmark(base.DBTable):
    manager = CopyManager
    method = 'copy'
    record_count = 100000
    datatypes = [
            'integer',
            'timestamp with time zone',
            'double precision',
            'varchar(12)',
            'bool',
        ]

    def generate_data(self, count):
        data = []
        for i in xrange(count):
            data.append((
                    i,
                    datetime(1970, 1, 1) + timedelta(hours=1),
                    math.pi * i,
                    hashlib.md5(str(i)).hexdigest()[:12],
                    0 == (i % 3),
                ))
        return data

    def benchmark(self):
        data = self.generate_data(self.record_count)
        cols = [base.numname(i) for i in range(len(self.datatypes))]
        mgr = self.manager(self.conn, self.table, cols)
        getattr(mgr, self.method)(data)
        cursor = self.conn.cursor()
        query = "SELECT count(*) FROM %s" % self.table
        cursor.execute(query)
        assert (cursor.fetchone()[0] == self.record_count)
        print "-" * 70
        print "%s execution times:" % self.__class__.__name__
        for name, time in mgr.times.iteritems():
            print "%30s: %.2fs" % (name, time)

class ThreadingBenchmark(Benchmark):
    method = 'threading_copy'
    manager = CopyManager
