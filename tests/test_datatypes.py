import itertools
from datetime import date, datetime
from unittest import TestCase
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
            self.assertEqual(a, self.cast(b))

    def cast(self, v): return v

class IntegerTest(TypeMixin, TestCase):
    datatypes = ['integer']

class BoolTest(TypeMixin, TestCase):
    datatypes = ['bool']

class SmallIntTest(TypeMixin, TestCase):
    datatypes = ['smallint']

class BigIntTest(TypeMixin, TestCase):
    datatypes = ['bigint']

class RealTest(TypeMixin, TestCase):
    datatypes = ['real']

class DoubleTest(TypeMixin, TestCase):
    datatypes = ['double precision']

class NullTest(TypeMixin, TestCase):
    null = 'NULL'
    datatypes = ['integer']
    data = [(1,), (2,), (None,)]

class VarcharTest(TypeMixin, TestCase):
    datatypes = ['varchar(12)']

class CharTest(TypeMixin, TestCase):
    datatypes = ['char(12)']

    def cast(self, v):
        self.assertEqual(12, len(v))
        return v.strip()

class TextTest(TypeMixin, TestCase):
    datatypes = ['text']
    data = [
        ('Fourscore and seven years ago our fathers set forth'
         'on this continent a new nation',),
        ('Python is a programming language that lets you work quickly'
         'and integrate systems more effectively.',),
    ]

class ByteaTest(TypeMixin, TestCase):
    datatypes = ['bytea']
    data = [
        ('\x02\xf3\x18\x44 alphabet',),
        ('The\x00hippopotamus\x00jumped\x00over\x00the\x00lazy\x00tower',)
    ,]

    def cast(self, v):
        self.assertIsInstance(v, buffer)
        return str(v)

class TimestampTest(TypeMixin, TestCase):
    datatypes = ['timestamp']

class TimestampTZTest(TypeMixin, TestCase):
    datatypes = ['timestamp with time zone']

    def cast(self, v):
        return util.to_utc(v)

class DateTest(TypeMixin, TestCase):
    datatypes = ['date']
