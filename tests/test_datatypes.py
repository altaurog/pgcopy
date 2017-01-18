import decimal
import itertools
from nose import tools as test
from pgcopy import CopyManager, util

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
            test.eq_(a, self.cast(b))

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
        test.eq_(12, len(v))
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
        test.assert_is_instance(v, buffer)
        return str(v)

class TestTimestamp(TypeMixin):
    datatypes = ['timestamp']

class TestTimestampTZ(TypeMixin):
    datatypes = ['timestamp with time zone']

    def cast(self, v):
        return util.to_utc(v)

class TestDate(TypeMixin):
    datatypes = ['date']

class TestNumeric(TypeMixin):
    datatypes = ['numeric']

    data = [
        (decimal.Decimal('100'),),
        (decimal.Decimal('-1000'),),
        (decimal.Decimal('21034.5678'),),
        (decimal.Decimal('-900000.0001'),),
    ]
