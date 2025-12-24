import pytest
from pgcopy import CopyManager

from . import test_datatypes


class TestThreadingCopy(test_datatypes.TypeMixin):
    record_count = 1
    datatypes = [
        "integer",
    ]

    def test_threading_copy(self, conn, cursor, schema_table, data):
        mgr = CopyManager(conn, self.table, self.cols)
        if not mgr.implements_threading_copy:
            pytest.skip("threading_copy not implemented")
        mgr.threading_copy(data)
        select_list = ",".join(self.cols)
        cursor.execute(self.select_sql(schema_table))
        self.checkResults(cursor, data)

    def test_threading_copy_error(self, conn, cursor):
        data = [{}]
        mgr = CopyManager(conn, self.table, self.cols)
        if not mgr.implements_threading_copy:
            pytest.skip("threading_copy not implemented")
        with pytest.raises(conn.DataError):
            mgr.threading_copy(data)

    def test_threading_copy_generator(self, conn, cursor, schema_table, data):
        mgr = CopyManager(conn, self.table, self.cols)
        if not mgr.implements_threading_copy:
            pytest.skip("threading_copy not implemented")
        mgr.threading_copy(iter(data))
        select_list = ",".join(self.cols)
        cursor.execute(self.select_sql(schema_table))
        self.checkResults(cursor, data)

    def test_threading_copy_empty_generator(self, conn, cursor, schema_table):
        data = []
        mgr = CopyManager(conn, self.table, self.cols)
        if not mgr.implements_threading_copy:
            pytest.skip("threading_copy not implemented")
        mgr.threading_copy(iter(data))
        select_list = ",".join(self.cols)
        cursor.execute(self.select_sql(schema_table))
        self.checkResults(cursor, data)
