from pgcopy import CopyManager
from . import test_datatypes

class TestPublicSchema(test_datatypes.TypeMixin):
    temp = ''
    datatypes = ['integer', 'bool', 'varchar(12)']

    def temp_schema_name(self):
        # This will set self.schema_table correctly, so that
        # TypeMixin.test_type will instantiate CopyManager
        # with public schema specified explicitly
        return "public"

    def test_default_public(self):
        # Use public schema by default
        bincopy = CopyManager(self.conn, self.table, self.cols)
        bincopy.copy(self.data)
        select_list = ','.join(self.cols)
        self.cur.execute("SELECT %s from %s" % (select_list, self.table))
        self.checkResults()

    def cast(self, v):
        if isinstance(v, str):
            return v.encode()
        return v
