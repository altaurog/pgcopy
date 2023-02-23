import psycopg2.sql

from pgcopy import CopyManager
from . import test_datatypes

class TestPublicSchema(test_datatypes.TypeMixin):
    tempschema = False
    datatypes = ['integer', 'bool', 'varchar(12)']

    def test_default_public(self, conn, cursor, data):
        bincopy = CopyManager(conn, self.table, self.cols)
        bincopy.copy(data)
        select_list = psycopg2.sql.SQL(",").join(self.cols)
        cursor.execute(psycopg2.sql.SQL("SELECT {} from public.{}").format(select_list, self.table))
        self.checkResults(cursor, data)

    def cast(self, v):
        if isinstance(v, str):
            return v.encode()
        return v


class TestCopyFallbackSchema(test_datatypes.TypeMixin):
    datatypes = ['integer', 'bool', 'varchar(12)']

    def test_fallback_schema_honors_search_path(self, conn, cursor, data, schema):
        cursor.execute(psycopg2.sql.SQL('SET search_path TO {}').format(psycopg2.sql.Identifier(schema)))
        bincopy = CopyManager(conn, self.table, self.cols)
        bincopy.copy(data)
        select_list = psycopg2.sql.SQL(',').join(map(psycopg2.sql.Identifier, self.cols))
        cursor.execute(psycopg2.sql.SQL("SELECT {} from {}").format(select_list, self.table))
        self.checkResults(cursor, data)

    def cast(self, v):
        if isinstance(v, str):
            return v.encode()
        return v
