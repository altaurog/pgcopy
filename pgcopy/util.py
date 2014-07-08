import re
import uuid
from datetime import datetime
from pytz import UTC

def to_utc(dt):
    if not isinstance(dt, datetime):
        dt = datetime(dt.year, dt.month, dt.day)
    if dt.tzinfo is None:
        return UTC.localize(dt)
    else:
        return dt.astimezone(UTC)


class Replace(object):
    """
    Utility for fast updates on table involving most rows in the table.
    Instead of executemany("UPDATE ..."), create and populate
    a new table (which can be done using COPY), then rename.

    Can do this only if no other objects in the db depend on the table.

    See http://dba.stackexchange.com/a/41111/9941
    """
    def __init__(self, connection, table):
        self.cursor = connection.cursor()
        self.uuid = uuid.uuid1()
        self.table = table
        self.temp_name = self.mangle(table)
        self.name_re = re.compile(r'\b%s\b' % re.escape(self.table))
        self.inspect()

    def __enter__(self):
        self.create_temp()
        return self.temp_name

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.create_notnull()
            self.create_constraints()
            self.create_indices()
            self.create_triggers()
            self.swap()
        self.cursor.close()

    def inspect(self):
        attquery = """
            SELECT a.attname FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            WHERE c.relname = %s AND a.attnum > 0 AND a.attnotnull
            """
        self.cursor.execute(attquery, (self.table,))
        self.notnull = [an for (an,) in self.cursor]
        # primary key is recreated as a constraint, 
        # but all other unique constraints are only
        # recreated as unique index
        conquery = """
            SELECT conname, pg_catalog.pg_get_constraintdef(r.oid, true)
            FROM pg_catalog.pg_constraint r
            JOIN pg_class c ON r.conrelid = c.oid
            WHERE c.relname = %s AND contype != 'u'
            """
        self.cursor.execute(conquery, (self.table,))
        self.constraints = self.cursor.fetchall()
        indquery = """
            SELECT pg_get_indexdef(indexrelid)
            FROM pg_index
            WHERE indrelid = %s::regclass
            AND NOT indisprimary
            """
        self.cursor.execute(indquery, (self.table,))
        self.indices = [i for (i,) in self.cursor.fetchall()]
        trigquery = """
            SELECT pg_get_triggerdef(oid)
            FROM pg_trigger
            WHERE tgrelid=%s::regclass
            AND NOT tgisconstraint
            """
        self.cursor.execute(trigquery, (self.table,))
        self.triggers = [t for (t,) in self.cursor.fetchall()]

    def create_temp(self):
        create = 'CREATE TABLE "%s" AS TABLE "%s" WITH NO DATA'
        self.cursor.execute(create % (self.temp_name, self.table))

    def create_notnull(self):
        nnsql = 'ALTER TABLE "%s" ALTER COLUMN "%s" SET NOT NULL'
        for col in self.notnull:
            self.cursor.execute(nnsql % (self.temp_name, col))

    def create_constraints(self):
        consql = 'ALTER TABLE "%s" ADD CONSTRAINT "%s" %s'
        for conname, condef in self.constraints:
            newname = self.mangle(conname)
            self.cursor.execute(consql % (self.temp_name, newname, condef))

    def create_indices(self):
        for indexsql in self.indices:
            self.cursor.execute(self.sqlrename(indexsql))

    def create_triggers(self):
        for trigsql in self.triggers:
            self.cursor.execute(self.sqlrename(trigsql))

    def swap(self):
        self.cursor.execute('DROP TABLE "%s"' % self.table)
        self.cursor.execute('ALTER TABLE "%s" RENAME TO %s'
                            % (self.temp_name, self.table))

    unsafe_re = re.compile(r'\W+')
    def mangle(self, name):
        base = '%s%s' % (name, self.uuid)
        return self.unsafe_re.sub('', base).lower()

    def sqlrename(self, sql):
        return self.name_re.sub(self.temp_name, sql)


