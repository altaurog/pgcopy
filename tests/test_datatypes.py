import itertools
from pgcopy import CopyManager, util

try:
    from pgcopy import ccopy
    import pandas as pd
    from datetime import datetime, time
except ImportError:
    ccopy = None

from . import db

class TypeMixin(db.TemporaryTable):
    null = 'NOT NULL'
    record_count = 3
    def test_type(self):
        bincopy = CopyManager(self.conn, self.table, self.cols)
        bincopy.copy(self.data)
        select_list = ','.join(self.cols)
        self.cur.execute("SELECT %s from %s" % (select_list, self.table))
        for rec in self.data:
            self.checkValues(rec, self.cur.fetchone())

    def checkValues(self, expected, found):
        for a, b in itertools.izip(expected, found):
            assert (a == self.cast(b))

    def cast(self, v): return v

class TestInteger(TypeMixin):
    datatypes = ['integer']

class TestBool(TypeMixin):
    datatypes = ['bool']

class TestSmallInt(TypeMixin):
    datatypes = ['smallint']

class TestBigInt(TypeMixin):
    datatypes = ['bigint']

class TestReal(TypeMixin):
    datatypes = ['real']

class TestDouble(TypeMixin):
    datatypes = ['double precision']

class TestNull(TypeMixin):
    null = 'NULL'
    datatypes = ['integer']
    data = [(1,), (2,), (None,)]

class TestVarchar(TypeMixin):
    datatypes = ['varchar(12)']

class TestChar(TypeMixin):
    datatypes = ['char(12)']

    def cast(self, v):
        assert (12 == len(v))
        return v.strip()

class TestText(TypeMixin):
    datatypes = ['text']
    data = [
        ('Fourscore and seven years ago our fathers set forth'
         'on this continent a new nation',),
        ('Python is a programming language that lets you work quickly'
         'and integrate systems more effectively.',),
    ]

class TestBytea(TypeMixin):
    datatypes = ['bytea']
    data = [
        ('\x02\xf3\x18\x44 alphabet',),
        ('The\x00hippopotamus\x00jumped\x00over\x00the\x00lazy\x00tower',)
    ,]

    def cast(self, v):
        assert isinstance(v, buffer)
        return str(v)

class TestTimestamp(TypeMixin):
    datatypes = ['timestamp']

class TestTimestampTZ(TypeMixin):
    datatypes = ['timestamp with time zone']

    def cast(self, v):
        return util.to_utc(v)

class TestDate(TypeMixin):
    datatypes = ['date']

if ccopy is not None:
    class CTypeMixin(TypeMixin):
        def test_type(self):
            bincopy = ccopy.CopyManager(self.conn, self.table, self.cols)
            bincopy.copy(self.dataframe())
            select_list = ','.join(self.cols)
            self.cur.execute("SELECT %s from %s" % (select_list, self.table))
            for rec in self.data:
                self.checkValues(rec, self.cur.fetchone())

        def dataframe(self):
            return pd.DataFrame(self.data, columns=self.cols)

    class TestCInteger(CTypeMixin):
        datatypes = ['integer']

    class TestCBool(CTypeMixin):
        datatypes = ['bool']

    class TestCSmallInt(CTypeMixin):
        datatypes = ['smallint']

    class TestCBigInt(CTypeMixin):
        datatypes = ['bigint']

    class TestCReal(CTypeMixin):
        datatypes = ['real']

    class TestCDouble(CTypeMixin):
        datatypes = ['double precision']

    class TestCNull(CTypeMixin):
        null = 'NULL'
        datatypes = ['integer']
        data = [(1,), (2,), (None,)]

    class TestCVarchar(CTypeMixin):
        datatypes = ['varchar(12)']

    class TestCChar(CTypeMixin):
        datatypes = ['char(12)']

        def cast(self, v):
            assert (12 == len(v))
            return v.strip()

    class TestCDate(CTypeMixin):
        def dataframe(self):
            t0 = time(0)
            data = [[datetime.combine(d, t0) for d in row] for row in self.data]
            return pd.DataFrame(data, columns=self.cols)

        datatypes = ['date']

    class TestCTimestamp(CTypeMixin):
        datatypes = ['timestamp']

    class TestCTimestampTZ(CTypeMixin):
        datatypes = ['timestamp with time zone']

        def cast(self, v):
            return util.to_utc(v)
