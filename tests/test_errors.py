from unittest import TestCase
from pgcopy import BinaryCopy
from . import get_conn

class TestErrors(TestCase):
    def test_nosuchcolumn(self):
        conn = get_conn()
        self.assertRaises(ValueError, BinaryCopy, conn, 'int_notnull', ['num'])
