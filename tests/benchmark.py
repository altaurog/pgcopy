import pandas as pd
from timeit import default_timer
from pgcopy import CopyManager
from . import db

class Benchmark(db.TemporaryTable):
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

    def do_copy(self):
        mgr = CopyManager(self.conn, self.table, self.cols)
        mgr.copy(self.dataframe().itertuples(False))

    def check_count(self):
        cursor = self.conn.cursor()
        query = "SELECT count(*) FROM %s" % self.table
        cursor.execute(query)
        assert (cursor.fetchone()[0] == self.record_count)

    def benchmark(self):
        start = default_timer()
        self.do_copy()
        self.elapsed_time = default_timer() - start
        self.check_count()

    def tearDown(self):
        super(Benchmark, self).tearDown()
        print "%30s: %6.02fs" % (self.__class__.__name__, self.elapsed_time)

class NullBenchmark(Benchmark):
    null = 'NULL'

class ExecuteManyBenchmark(Benchmark):
    def do_copy(self):
        cols = ','.join(self.cols)
        paramholders = ','.join(['%s'] * len(self.cols))
        sql = "INSERT INTO %s (%s) VALUES (%s)" \
                % (self.table, cols, paramholders)
        cursor = self.conn.cursor()
        cursor.executemany(sql, self.data)

class NullExecuteManyBenchmark(ExecuteManyBenchmark):
    null = 'NULL'

try:
    from pgcopy import ccopy

    class CythonBenchmark(Benchmark):
        def do_copy(self):
            mgr = ccopy.CopyManager(self.conn, self.table, self.cols)
            mgr.copy(self.dataframe())

    class NullCythonBenchmark(CythonBenchmark):
        null = 'NULL'

except ImportError:
    pass
