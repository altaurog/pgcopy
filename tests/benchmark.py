import pandas as pd
from timeit import default_timer
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
        self.print_results(mgr.times.iteritems())

    def print_results(self, results):
        print "-" * 70
        print "%s execution times:" % self.__class__.__name__
        for name, elapsed_time in results:
            print "%30s: %.2fs" % (name, elapsed_time)

class NullBenchmark(Benchmark):
    null = 'NULL'

class ExecuteManyBenchmark(Benchmark):
    def benchmark(self):
        cols = ','.join(self.cols)
        paramholders = ','.join(['%s'] * len(self.cols))
        sql = "INSERT INTO %s (%s) VALUES (%s)" \
                % (self.table, cols, paramholders)
        cursor = self.conn.cursor()
        start = default_timer()
        cursor.executemany(sql, self.data)
        elapsed = default_timer() - start
        self.check_count()
        self.print_results([('executemany', elapsed)])

class NullExecuteManyBenchmark(ExecuteManyBenchmark):
    null = 'NULL'

try:
    from pgcopy import ccopy

    class CythonBenchmark(Benchmark):
        manager = ccopy.CopyManager
        def do_copy(self, mgr):
            df = self.dataframe()
            mgr.copy(df)

    class NullCythonBenchmark(CythonBenchmark):
        null = 'NULL'

except ImportError:
    pass
