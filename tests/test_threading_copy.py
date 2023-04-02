import pytest

from psycopg2.errors import BadCopyFileFormat

from pgcopy import CopyManager

from . import test_datatypes


class TestThreadingCopy(test_datatypes.TypeMixin):
    record_count = 1
    datatypes = [
        "integer",
    ]

    def test_threading_copy(self, conn, cursor, data):
        mgr = CopyManager(conn, self.table, self.cols)
        mgr.threading_copy(data)
        select_list = ",".join(self.cols)
        cursor.execute("SELECT %s from %s" % (select_list, self.table))
        self.checkResults(cursor, data)

    def test_threading_copy_error(self, conn, cursor):
        data = [{}]
        mgr = CopyManager(conn, self.table, self.cols)
        with pytest.raises(BadCopyFileFormat):
            mgr.threading_copy(data)

    @staticmethod
    def gen_data(data):
        for item in data:
            yield item

    def test_threading_copy_generator(self, conn, cursor, data):
        mgr = CopyManager(conn, self.table, self.cols)
        mgr.threading_copy(self.gen_data(data))
        select_list = ",".join(self.cols)
        cursor.execute("SELECT %s from %s" % (select_list, self.table))
        self.checkResults(cursor, data)

    def test_threading_copy_empty_generator(self, conn, cursor):
        data = []
        mgr = CopyManager(conn, self.table, self.cols)
        mgr.threading_copy(self.gen_data(data))
        select_list = ",".join(self.cols)
        cursor.execute("SELECT %s from %s" % (select_list, self.table))
        self.checkResults(cursor, data)

