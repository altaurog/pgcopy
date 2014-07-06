import pandas as pd
from pgcopy import CopyManager
from . import db

class Benchmark(db.TemporaryTable):
    manager = CopyManager
    record_count = 100000
    datatypes = [
            'integer',
            'timestamp with time zone',
            'double precision',
            'varchar(12)',
            'bool',
        ]

    def dataframe(self):
        df = pd.DataFrame(self.data, columns=self.cols)
        return df

    def do_copy(self, mgr):
        mgr.copy(self.dataframe().itertuples(False))

    def check_count(self):
        cursor = self.conn.cursor()
        query = "SELECT count(*) FROM %s" % self.table
        cursor.execute(query)
        assert (cursor.fetchone()[0] == self.record_count)

    def benchmark(self):
        mgr = self.manager(self.conn, self.table, self.cols)
        self.do_copy(mgr)
        self.check_count()
        print "-" * 70
        print "%s execution times:" % self.__class__.__name__
        for name, time in mgr.times.iteritems():
            print "%30s: %.2fs" % (name, time)

try:
    import pyximport
    pyximport.install()
    from pgcopy import ccopy

    class CythonBenchmark(Benchmark):
        manager = ccopy.CopyManager
        def do_copy(self, mgr):
            df = self.dataframe()
            mgr.copy(df)
except ImportError:
    pass
