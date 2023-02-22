from io import BytesIO

from pgcopy import CopyManager

from .test_datatypes import TypeMixin


class Test(TypeMixin):
    datatypes = ["integer", "bool", "varchar(12)"]

    def test_copy(self, conn, cursor, schema_table, data):
        bincopy = CopyManager(conn, schema_table, self.cols)
        bincopy.copy(data, BytesIO)
        select_list = ",".join(self.cols)
        schema, table = schema_table.split(".")
        cursor.execute('SELECT %s from "%s"."%s"' % (select_list, schema, table))
        self.checkResults(cursor, data)

    def cast(self, v):
        try:
            return v.encode()
        except AttributeError:
            return v
