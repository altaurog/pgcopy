import json
import decimal
import sys
import uuid


if sys.version_info < (3,):
    memoryview = buffer

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
        self.checkResults()

    def checkResults(self):
        for rec in self.data:
            self.checkValue(rec, self.cur.fetchone())

    def checkValue(self, expected, found):
        for a, b in zip(expected, found):
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

    def cast(self, v):
        return v.strip().encode()

class TestChar(TypeMixin):
    datatypes = ['char(12)']

    def cast(self, v):
        test.eq_(12, len(v))
        return v.strip().encode()

class TestText(TypeMixin):
    datatypes = ['text']
    data = [
        (b'Fourscore and seven years ago our fathers set forth '
         b'on this continent a new nation',),
        (b'Python is a programming language that lets you work quickly '
         b'and integrate systems more effectively.',),
    ]

    def cast(self, v):
        return v.encode()


class TestJSON(TypeMixin):
    datatypes = ['json']
    data = [
        (b'{"data": "Fourscore and seven years ago our fathers set forth"}',),
        (b'{"data": "Python is a programming language that lets you work quickly"}',),
    ]

    def cast(self, v):
        return json.dumps(v).encode()


class TestJSONB(TypeMixin):
    datatypes = ['jsonb']
    data = [
        (b'{"data": "Fourscore and seven years ago our fathers set forth"}',),
        (b'{"data": "Python is a programming language that lets you work quickly"}',),
    ]

    def cast(self, v):
        return json.dumps(v).encode()


class TestBytea(TypeMixin):
    datatypes = ['bytea']
    data = [
        (b'\x02\xf3\x18\x44 alphabet',),
        (b'The\x00hippopotamus\x00jumped\x00over\x00the\x00lazy\x00tower',)
    ,]

    def cast(self, v):
        test.assert_is_instance(v, memoryview)
        return bytes(v)

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
        (decimal.Decimal('21034.56'),),
        (decimal.Decimal('-900000.0001'),),
    ]

class TestNumericNan(TypeMixin):
    datatypes = ['numeric']
    data = [(decimal.Decimal('NaN'),),]

    def checkResults(self):
        assert self.cur.fetchone()[0].is_nan()


class TestUUID(TypeMixin):
    datatypes = ['uuid']
    data = [
        (uuid.UUID('55daa192-a28a-4c49-ae84-ef3564e32308'),),
        (uuid.UUID('8b56420e-7e15-4bae-a76e-af20e35ea88f'),),
    ]

    def cast(self, v):
        return uuid.UUID(v)
