from datetime import datetime
import random
import string
import time
from timeit import default_timer
from pgcopy import CopyManager
from . import base

class TimerMixin(object):
    def setUp(self):
        super(TimerMixin, self).setUp()
        self.times = {}
        random.seed()
        for cls, attrname in self.capture:
            self.wrap(cls, attrname)

    def wrap(self, cls, attrname):
        name = "%s.%s" % (cls.__name__, attrname)
        self.times.setdefault(name, 0)
        func = getattr(cls, attrname)
        def wrapper(*args, **kwargs):
            start = default_timer()
            result = func(*args, **kwargs)
            self.times[name] += default_timer() - start
            return result
        setattr(cls, attrname, wrapper)

    def tearDown(self):
        super(TimerMixin, self).tearDown()
        print "-" * 70
        print "%s execution times:" % self.__class__.__name__
        for name, time in self.times.iteritems():
            print "%30s: %.2fs" % (name, time)


mints = time.mktime(datetime(1970, 1, 1).timetuple())
maxts = time.mktime(datetime(2038, 1, 1).timetuple())
class Benchmark(TimerMixin, base.DBTable):
    manager = CopyManager
    method = 'copy'
    datatypes = [
            'integer',
            'timestamp with time zone',
            'double precision',
            'varchar(12)',
            'bool',
        ]

    capture = [
            (manager, 'writestream'),
            (manager, 'copystream'),
            (manager, 'copy'),
            (manager, 'threading_copy'),
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
        data = self.generate_data(100000)
        cols = [base.numname(i) for i in range(len(self.datatypes))]
        mgr = self.manager(self.conn, self.table, cols)
        getattr(mgr, self.method)(data)

class ThreadingBenchmark(Benchmark):
    method = 'threading_copy'
