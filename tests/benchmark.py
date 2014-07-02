from datetime import datetime
import random
import string
import time
from pgcopy import CopyManager
from . import base


mints = time.mktime(datetime(1970, 1, 1).timetuple())
maxts = time.mktime(datetime(2038, 1, 1).timetuple())
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
                    random.randint(-2147483648, +2147483647),
                    datetime.fromtimestamp(random.uniform(mints, maxts)),
                    random.uniform(-1e4,1e8),
                    ''.join(random.sample(string.printable, random.randrange(1,13))),
                    random.choice((True, False)),
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
