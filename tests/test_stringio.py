from io import BytesIO
from pgcopy import CopyManager, util
from .test_datatypes import TypeMixin

class Test(TypeMixin):
    datatypes = ['integer', 'bool', 'varchar(12)']

    def test_copy(self):
        bincopy = CopyManager(self.conn, self.schema_table, self.cols)
        bincopy.copy(self.data, BytesIO)
        select_list = ','.join(self.cols)
        self.cur.execute("SELECT %s from %s" % (select_list, self.table))
        self.checkResults()

    def cast(self, v):
        try:
            return v.encode()
        except AttributeError:
            return v
