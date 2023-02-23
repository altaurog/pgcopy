from timeit import default_timer

from pgcopy import CopyManager

from . import db


class PGCopyBenchmark(db.TemporaryTable):
    record_count = 100000
    datatypes = [
        "integer",
        "timestamp with time zone",
        "double precision",
        "varchar(12)",
        "bool",
    ]

    def do_copy(self):
        mgr = CopyManager(self.conn, self.schema_table, self.cols)
        mgr.copy(self.data)

    def check_count(self):
        cursor = self.conn.cursor()
        query = "SELECT count(*) FROM %s" % self.schema_table
        cursor.execute(query)
        assert cursor.fetchone()[0] == self.record_count

    def benchmark(self):
        start = default_timer()
        self.do_copy()
        self.elapsed_time = default_timer() - start
        self.check_count()

    def teardown(self):
        super(PGCopyBenchmark, self).teardown()
        print("%30s: %6.02fs" % (self.__class__.__name__, self.elapsed_time))


class ExecuteManyBenchmark(PGCopyBenchmark):
    def setup(self):
        super(ExecuteManyBenchmark, self).setup()
        decode = lambda t: (t[0], t[1], t[2], t[3].decode(), t[4])
        self.data = [decode(d) for d in self.data]

    def do_copy(self):
        cols = ",".join(self.cols)
        paramholders = ",".join(["%s"] * len(self.cols))
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (
            self.schema_table,
            cols,
            paramholders,
        )
        cursor = self.conn.cursor()
        cursor.executemany(sql, self.data)


if __name__ == "__main__":
    for cls in ExecuteManyBenchmark, PGCopyBenchmark:
        benchmark = cls()
        benchmark.setup()
        benchmark.benchmark()
        benchmark.teardown()
