import itertools
from datetime import date, datetime
from unittest import TestCase
from pgcopy import BinaryCopy, util
from . import get_conn

class TypeMixin(object):
    null = 'NOT NULL'
    def setUp(self):
        self.conn = get_conn()
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
        self.cur.execute("CREATE TEMPORARY TABLE typetest (id %s %s);"
                        % (self.datatype, self.null))

    def tearDown(self):
        self.conn.rollback()

    def test_type(self):
        bincopy = BinaryCopy(self.conn, 'typetest', ['id'])
        bincopy.copy(self.data)
        self.cur.execute("SELECT id from typetest")
        for rec in self.data:
            self.checkValues(rec, self.cur.fetchone())

    def checkValues(self, expected, found):
        for a, b in itertools.izip(expected, found):
            self.assertEqual(a, self.cast(b))

    def cast(self, v): return v

class IntegerTest(TypeMixin, TestCase):
    data = [(1,), (2,)]
    datatype = 'integer'

class BoolTest(TypeMixin, TestCase):
    data = [(True,), (False,)]
    datatype = 'bool'

class SmallIntTest(TypeMixin, TestCase):
    data = [(1,), (2,)]
    datatype = 'smallint'

class BigIntTest(TypeMixin, TestCase):
    data = [(1,), (2,)]
    datatype = 'bigint'

class RealTest(TypeMixin, TestCase):
    data = [(1.5,), (2.25,)]
    datatype = 'real'

class DoubleTest(TypeMixin, TestCase):
    data = [(1.5,), (2.25,)]
    datatype = 'double precision'

class NullTest(TypeMixin, TestCase):
    null = 'NULL'
    data = [(1,), (2,), (None,)]
    datatype = 'integer'

class VarcharTest(TypeMixin, TestCase):
    data = [('one',), ('three',)]
    datatype = 'varchar(10)'

class CharTest(TypeMixin, TestCase):
    data = [('one',), ('three',)]
    datatype = 'char(5)'

    def cast(self, v):
        self.assertEqual(5, len(v))
        return v.strip()

class TextTest(TypeMixin, TestCase):
    data = [
        ('Fourscore and seven years ago our fathers set forth'
         'on this continent a new nation',),
        ('Python is a programming language that lets you work quickly'
         'and integrate systems more effectively.',),
    ]
    datatype = 'text'

class ByteaTest(TypeMixin, TestCase):
    data = [
        ('\x02\xf3\x18\x44 alphabet',),
        ('The\x00hippopotamus\x00jumped\x00over\x00the\x00lazy\x00tower',)
    ,]
    datatype = 'bytea'

    def cast(self, v):
        self.assertIsInstance(v, buffer)
        return str(v)

class TimestampTest(TypeMixin, TestCase):
    data = [(datetime.now(),), (datetime(1974, 8, 21, 6, 30),) ]
    datatype = 'timestamp'

class TimestampTZTest(TypeMixin, TestCase):
    data = [
            (util.to_utc(datetime.now()),),
            (util.to_utc(datetime(1974, 8, 21, 6, 30)),)
        ]
    datatype = 'timestamp with time zone'

    def cast(self, v):
        return util.to_utc(v)

class DateTest(TypeMixin, TestCase):
    data = [(date(2003, 5, 24),), (date.today(),)]
    datatype = 'date'
