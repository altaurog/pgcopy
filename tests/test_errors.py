from unittest import TestCase
from pgcopy import CopyManager
from . import get_conn

class TestErrors(TestCase):
    def test_nosuchcolumn(self):
        conn = get_conn()
        self.assertRaises(ValueError, CopyManager, conn, 'int_notnull', ['num'])
