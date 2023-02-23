import collections
import re
import random
import string
from collections.abc import Iterable
from datetime import datetime, time
from six import string_types

from pytz import UTC
import psycopg2.sql


def array_info(arr):
    """
    returns ndims, *lengths
    """
    if not isinstance(arr, (list, tuple, set)):
        return 0,
    subs = set([array_info(elem) for elem in arr])
    if len(subs) > 1:
        raise ValueError('subarray dimensions must match')
    if len(subs) == 0:
        return 1, 0
    s = subs.pop()
    dim, lengths = s[0], s[1:]
    return (dim + 1, len(arr)) + lengths

def array_iter(arr):
    for i in arr:
        if isinstance(i, Iterable) and not isinstance(i, string_types):
            for x in array_iter(i):
                yield x
        else:
            yield i

def get_schema(conn, table):
    cur = conn.cursor()
    query = psycopg2.sql.SQL("""
        SELECT n.nspname, c.relname
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = %s::regclass
        """)
    cur.execute(query, (psycopg2.sql.Identifier(table),))
    return cur.fetchone()[0]

def to_utc(dt):
    if not isinstance(dt, datetime):
        dt = datetime(dt.year, dt.month, dt.day)
    if dt.tzinfo is None:
        return UTC.localize(dt)
    else:
        return dt.astimezone(UTC)

def to_utc_time(t):
    if not isinstance(t, time):
        t = time(t.hour, t.minute, t.second, t.microsecond)
    
    return UTC.localize(t)

source = string.ascii_lowercase + string.digits
def uid():
    return ''.join(random.choice(source) for _ in range(5))


idre = lambda name: re.compile(r'\b{}\b'.format(re.escape(name)))

class Replace(object):
    """
    Context manager for fast updates on table involving most rows in the table.
    Instead of `executemany("UPDATE ...")`, create and populate
    a new table (which can be done using COPY), then rename.
    This is possible only if no other tables in the db depend on the table.

    :param connection: database connection
    :type connection: psycopg2 connection

    :param table: the table name.  Schema may be specified using dot notation: ``schema.table``.
    :type table: str

    On entry, it creates a new table like the original, with a
    temporary name.  Default column values are included.

    On exit, it recreates the constraints, indices, triggers, and views on
    the new table, then replaces the old table with the new::

        from pgcopy import CopyManager, Replace
        with Replace(conn, 'mytable') as temp_name:
            mgr = CopyManager(conn, temp_name, cols)
            mgr.copy(records)

    New db objects are named like the old, where possible.
    Names of foreign key and check constraints will be mangled.

    .. note:: on PostgreSQL 9.1 and earlier, concurrent queries on the table
        `will fail`_ once the table is dropped.

    .. _will fail: https://gist.github.com/altaurog/ab0019837719d2a93e6b
    """
    def __init__(self, connection, table):
        self.cursor = connection.cursor()
        self.uid = uid()
        if '.' in table:
            self.schema, self.table = table.rsplit('.', 1)
        else:
            self.schema, self.table = get_schema(connection, table), table
        self.schema_qualified_table = psycopg2.sql.Identifier(self.schema, self.table)
        self.schema = psycopg2.sql.Identifier(self.schema)
        self.table = psycopg2.sql.Identifier(self.table)
        self.name_re = idre(self.table)
        self.temp_name = psycopg2.sql.Identifier(self.newname())
        self.rename = [('TABLE', self.temp_name, self.table)]
        self.inspect()

    def __enter__(self):
        self.create_temp()
        self.create_defaults()
        return self.temp_name

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.create_notnull()
            self.create_constraints()
            self.create_indices()
            self.create_triggers()
            self.swap()
            self.create_views()
        self.cursor.close()

    def inspect(self):
        defquery = psycopg2.sql.SQL("""
            SELECT attname, pg_catalog.pg_get_expr(adbin, adrelid)
            FROM pg_catalog.pg_attribute
            JOIN pg_catalog.pg_attrdef ON attrelid = adrelid AND adnum = attnum
            WHERE adnum > 0
            AND attrelid = %s::regclass
            """)
        self.cursor.execute(defquery, (self.table,))
        self.defaults = self.cursor.fetchall()
        seqquery = psycopg2.sql.SQL("""
            SELECT attname, relname
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_depend ON (objid = c.oid)
            JOIN pg_catalog.pg_attribute
                ON (attnum=refobjsubid AND attrelid=refobjid)
            WHERE relkind = 'S'
            AND refobjid = %s::regclass
            """)
        self.cursor.execute(seqquery, (self.table,))
        self.sequences = self.cursor.fetchall()
        attquery = psycopg2.sql.SQL("""
            SELECT attname
            FROM pg_catalog.pg_attribute
            WHERE attrelid = %s::regclass
            AND attnum > 0 AND attnotnull
            """)
        self.cursor.execute(attquery, (self.table,))
        self.notnull = [an for (an,) in self.cursor]
        # primary key is recreated as a constraint, 
        # but all other unique constraints are only
        # recreated as unique index
        conquery = psycopg2.sql.SQL("""
            SELECT DISTINCT contype, conname, pg_catalog.pg_get_constraintdef(oid)
            FROM pg_catalog.pg_constraint
            WHERE conrelid = %s::regclass AND contype != 'u'
            """)
        self.cursor.execute(conquery, (self.table,))
        self.constraints = self.cursor.fetchall()
        indquery = psycopg2.sql.SQL("""
            SELECT c.relname, pg_catalog.pg_get_indexdef(i.indexrelid)
            FROM pg_catalog.pg_index i
            JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
            WHERE NOT indisprimary
            AND indrelid = %s::regclass
            """)
        self.cursor.execute(indquery, (self.table,))
        self.indices = self.cursor.fetchall()
        trigquery = psycopg2.sql.SQL("""
            SELECT tgname, pg_catalog.pg_get_triggerdef(oid)
            FROM pg_catalog.pg_trigger
            WHERE tgconstraint = 0
            AND tgrelid=%s::regclass
            """)
        self.cursor.execute(trigquery, (self.table,))
        self.triggers = self.cursor.fetchall()
        viewquery = psycopg2.sql.SQL("""
            SELECT DISTINCT n.nspname, c.relname, pg_catalog.pg_get_viewdef(r.ev_class)
            FROM pg_catalog.pg_rewrite r
            JOIN pg_catalog.pg_depend d ON d.objid = r.oid
            JOIN pg_catalog.pg_class c ON c.oid = r.ev_class
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE d.refobjid = %s::regclass;
            """)
        self.cursor.execute(viewquery, (self.table,))
        self.views = self.cursor.fetchall()

    def create_temp(self):
        create = psycopg2.sql.SQL('CREATE TABLE {} AS TABLE {} WITH NO DATA')
        self.cursor.execute(create.format(
            self.temp_name,
            self.schema_qualified_table
        ))

    def create_defaults(self):
        defsql = psycopg2.sql.SQL('ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}')
        for col, default in self.defaults:
            self.cursor.execute(defsql.format(
                self.temp_name,
                psycopg2.sql.Identifier(col),
                default
            ))

    def create_notnull(self):
        nnsql = psycopg2.sql.SQL('ALTER TABLE {} ALTER COLUMN {} SET NOT NULL')
        for col in self.notnull:
            self.cursor.execute(nnsql.format(self.temp_name,
                                             psycopg2.sql.Identifier(col)))

    def create_constraints(self):
        consql = psycopg2.sql.SQL('ALTER TABLE {} ADD CONSTRAINT {} {}')
        for i, (contype, conname, condef) in enumerate(self.constraints):
            newname = self.newname('con', i)
            self.cursor.execute(consql.format(
                self.temp_name, newname, condef
            ))
            if 'p' == contype:
                self.rename.append((
                    'INDEX', psycopg2.sql.Identifier(newname), conname
                ))

    def create_indices(self):
        for i, (oldidxname, indexsql) in enumerate(self.indices):
            newidxname = self.newname('idx', i)
            newsql = psycopg2.sql.SQL(self.sqlrename(indexsql, oldidxname, newidxname))
            self.cursor.execute(newsql)
            self.rename.append(
                ('INDEX',
                 psycopg2.sql.Identifier(newidxname),
                 oldidxname)
            )

    def create_triggers(self):
        for i, (oldtrigname, trigsql) in enumerate(self.triggers):
            newtrigname = self.newname('tg', i)
            newsql = psycopg2.sql.SQL(self.sqlrename(trigsql, oldtrigname, newtrigname))
            self.cursor.execute(newsql)
            self.rename.append(
                (
                    'TRIGGER',
                    psycopg2.sql.SQL('{} ON {}').format(
                        psycopg2.sql.Identifier(self.schema, newtrigname),
                        psycopg2.sql.Identifier(self.schema, oldtrigname)
                    )
                )
            )

    def swap(self):
        self.drop_views()
        self.drop_defaults()
        self.move_sequences()
        self.drop_original_table()
        self.rename_temp_table()

    def drop_views(self):
        for schema, viewname, viewdef in self.views:
            view_name = psycopg2.sql.Identifier(schema, viewname)
            sql = psycopg2.sql.SQL('DROP VIEW {}').format(view_name)
            self.cursor.execute(sql)

    def drop_defaults(self):
        dropdefsql = psycopg2.sql.SQL('ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT')
        for col, default in self.defaults:
            self.cursor.execute(dropdefsql.format(
                self.schema_qualified_table, psycopg2.sql.Identifier(col)
            ))

    def move_sequences(self):
        seqownersql = psycopg2.sql.SQL('ALTER SEQUENCE {} OWNED BY {}')
        for col, seq in self.sequences:
            self.cursor.execute(seqownersql.format(
                psycopg2.sql.Identifier(seq, self.temp_name),
                psycopg2.sql.Identifier(col)
            ))

    def drop_original_table(self):
        self.cursor.execute(psycopg2.sql.SQL('DROP TABLE {}').format(self.schema_qualified_table))

    def rename_temp_table(self):
        template = psycopg2.sql.SQL('ALTER {} {} RENAME TO {}')
        for obj_type, oldname, newname in self.rename:
            self.cursor.execute(template.format(
                obj_type, oldname, newname
            ))

    def create_views(self):
        viewsql = psycopg2.sql.SQL('CREATE VIEW {} AS {}')
        for schema, viewname, viewdef in self.views:
            sql = viewsql.format(psycopg2.sql.Identifier(schema, viewname),
                                 viewdef)
            self.cursor.execute(sql)


    unsafe_re = re.compile(r'\W+')
    def newname(self, pre=None, i=None):
        parts = ['%s']
        vals = [self.table]
        if pre is not None:
            parts.append('%s')
            vals.append(pre)
        if i is not None:
            parts.append('%02d')
            vals.append(i)
        parts.append('%s')
        vals.append(self.uid)
        return self.unsafe_re.sub('', '_'.join(parts) % tuple(vals)).lower()

    def sqlrename(self, sql, *args):
        newsql = self.name_re.sub(self.temp_name, sql)
        try:
            old, new = args
        except ValueError:
            return newsql
        else:
            return idre(old).sub(new, newsql)


class RenameReplace(Replace):
    """
    Subclass of :class:`Replace` whic renaming original table instead of dropping it.

    :param connection: database connection
    :type connection: psycopg2 connection

    :param table: the table name.  Schema may be specified using dot notation: ``schema.table``.
    :type table: str

    :param xform: a function translating original table name to new name, used for table and pk constraint names
    :type xform: function

    ::

        from pgcopy import CopyManager
        from pgcopy.util import RenameReplace
        xform = lambda s: s + '_old'
        with RenameReplace(conn, 'mytable', xform) as temp_name:
            mgr = CopyManager(conn, temp_name, cols)
            mgr.copy(records)
    """
    def __init__(self, connection, table, xform):
        """
        xform must be a function which translates old
        names to new ones, used on tables & pk constraints
        """
        super(RenameReplace, self).__init__(connection, table)
        self.xform = xform

    def drop_original_table(self):
        pass

    def rename_temp_table(self):
        sql = psycopg2.sql.SQL('ALTER {} {} RENAME TO {}')
        for objtype, temp, orig in self.rename:
            new_name = self.xform(orig)
            print("orig %{}%\n"
                  "new_name %{}%\n".format(orig, new_name))
            self.cursor.execute(sql.format(
                objtype,
                orig, new_name
            ))
        super(RenameReplace, self).rename_temp_table()


def rename_replace(connection, table, xform):
    with RenameReplace(connection, table, xform):
        pass
