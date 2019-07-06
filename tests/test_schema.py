from pgcopy import CopyManager
from . import test_datatypes

class TestPublicSchema(test_datatypes.TypeMixin):
    tempschema = False
    datatypes = ['integer', 'bool', 'varchar(12)']

    def test_default_public(self):
        bincopy = CopyManager(self.conn, self.table, self.cols)
        bincopy.copy(self.data)
        select_list = ','.join(self.cols)
        self.cur.execute("SELECT %s from public.%s" % (select_list, self.table))
        self.checkResults()

    def cast(self, v):
        if isinstance(v, str):
            return v.encode()
        return v


class TestCopyFallbackSchema(test_datatypes.TypeMixin):
    datatypes = ['integer', 'bool', 'varchar(12)']

    def test_fallback_schema_honors_search_path(self):
        self.cur.execute('SET search_path TO {}'.format(self.schema))
        bincopy = CopyManager(self.conn, self.table, self.cols)
        bincopy.copy(self.data)
        select_list = ','.join(self.cols)
        self.cur.execute("SELECT %s from %s" % (select_list, self.table))
        self.checkResults()

    def cast(self, v):
        if isinstance(v, str):
            return v.encode()
        return v
