import re
import random
import string
from datetime import datetime
from pytz import UTC

def to_utc(dt):
    if not isinstance(dt, datetime):
        dt = datetime(dt.year, dt.month, dt.day)
    if dt.tzinfo is None:
        return UTC.localize(dt)
    else:
        return dt.astimezone(UTC)

source = string.ascii_lowercase + string.digits
def uid():
    vals = [random.choice(source) for i in range(5)]
    return ''.join(vals)


idre = lambda name: re.compile(r'\b%s\b' % re.escape(name))

class Replace(object):
    """
    Utility for fast updates on table involving most rows in the table.
    Instead of executemany("UPDATE ..."), create and populate
    a new table (which can be done using COPY), then rename.

    Can do this only if no other tables in the db depend on the table.

    NOTE: With PostgreSQL < 9.3 after the table is dropped attempts to
    query it will fail.

    See http://dba.stackexchange.com/a/41111/9941
    """
    def __init__(self, connection, table):
        self.cursor = connection.cursor()
        self.uid = uid()
        if '.' in table:
            self.schema, self.table = table.rsplit('.', 1)
        else:
            self.schema, self.table = 'public', table
        self.name_re = idre(table)
        self.temp_name = self.newname()
        self.rename = [('TABLE', self.nameformat(self.temp_name), self.table)]
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
        defquery = """
            SELECT attname, pg_get_expr(adbin, adrelid)
            FROM pg_catalog.pg_attribute
            JOIN pg_catalog.pg_attrdef ON attrelid = adrelid AND adnum = attnum
            WHERE adnum > 0
            AND attrelid = %s::regclass
            """
        self.cursor.execute(defquery, (self.nameformat(self.table),))
        self.defaults = self.cursor.fetchall()
        seqquery = """
            SELECT attname, relname
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_depend ON (objid = c.oid)
            JOIN pg_catalog.pg_attribute
                ON (attnum=refobjsubid AND attrelid=refobjid)
            WHERE relkind = 'S'
            AND refobjid = %s::regclass
            """
        self.cursor.execute(seqquery, (self.nameformat(self.table),))
        self.sequences = self.cursor.fetchall()
        attquery = """
            SELECT attname
            FROM pg_catalog.pg_attribute
            WHERE attrelid = %s::regclass
            AND attnum > 0 AND attnotnull
            """
        self.cursor.execute(attquery, (self.nameformat(self.table),))
        self.notnull = [an for (an,) in self.cursor]
        # primary key is recreated as a constraint, 
        # but all other unique constraints are only
        # recreated as unique index
        conquery = """
            SELECT DISTINCT contype, conname, pg_catalog.pg_get_constraintdef(oid)
            FROM pg_catalog.pg_constraint
            WHERE conrelid = %s::regclass AND contype != 'u'
            """
        self.cursor.execute(conquery, (self.nameformat(self.table),))
        self.constraints = self.cursor.fetchall()
        indquery = """
            SELECT c.relname, pg_catalog.pg_get_indexdef(i.indexrelid)
            FROM pg_catalog.pg_index i
            JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
            WHERE NOT indisprimary
            AND indrelid = %s::regclass
            """
        self.cursor.execute(indquery, (self.nameformat(self.table),))
        self.indices = self.cursor.fetchall()
        trigquery = """
            SELECT tgname, pg_catalog.pg_get_triggerdef(oid)
            FROM pg_catalog.pg_trigger
            WHERE tgconstraint = 0
            AND tgrelid=%s::regclass
            """
        self.cursor.execute(trigquery, (self.nameformat(self.table),))
        self.triggers = self.cursor.fetchall()
        viewquery = """
            SELECT DISTINCT n.nspname, c.relname, pg_get_viewdef(r.ev_class)
            FROM pg_catalog.pg_rewrite r
            JOIN pg_catalog.pg_depend d ON d.objid = r.oid
            JOIN pg_catalog.pg_class c ON c.oid = r.ev_class
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE d.refobjid = %s::regclass;
            """
        self.cursor.execute(viewquery, (self.nameformat(self.table),))
        self.views = self.cursor.fetchall()

    def create_temp(self):
        create = 'CREATE TABLE {} AS TABLE {} WITH NO DATA'
        self.cursor.execute(create.format(
            self.nameformat(self.temp_name),
            self.nameformat(self.table)
        ))

    def create_defaults(self):
        defsql = 'ALTER TABLE {} ALTER COLUMN "{}" SET DEFAULT {}'
        for col, default in self.defaults:
            self.cursor.execute(defsql.format(
                self.nameformat(self.temp_name), col, default
            ))

    def create_notnull(self):
        nnsql = 'ALTER TABLE {} ALTER COLUMN "{}" SET NOT NULL'
        for col in self.notnull:
            self.cursor.execute(nnsql.format(self.nameformat(self.temp_name), col))

    def create_constraints(self):
        consql = 'ALTER TABLE {} ADD CONSTRAINT "{}" {}'
        for i, (contype, conname, condef) in enumerate(self.constraints):
            newname = self.newname('con', i)
            self.cursor.execute(consql.format(
                self.nameformat(self.temp_name), newname, condef
            ))
            if 'p' == contype:
                pass  # self.rename.append(('INDEX', self.nameformat(newname), conname))

    def create_indices(self):
        for i, (oldidxname, indexsql) in enumerate(self.indices):
            newidxname = self.newname('idx', i)
            newsql = self.sqlrename(indexsql, oldidxname, newidxname)
            self.cursor.execute(newsql)
            # self.rename.append(('INDEX', self.nameformat(newidxname), oldidxname))

    def create_triggers(self):
        for i, (oldtrigname, trigsql) in enumerate(self.triggers):
            newtrigname = self.newname('tg', i)
            newsql = self.sqlrename(trigsql, oldtrigname, newtrigname)
            self.cursor.execute(newsql)
            # self.rename.append((
            #     'TRIGGER',
            #     '%s ON %s' % (newtrigname, self.nameformat(self.table)),
            #     oldtrigname,
            # ))

    def swap(self):
        self.cursor.execute("""
            SELECT c.relkind, n.nspname, c.relname, ic.relname
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_index i
                ON c.oid = i.indexrelid
            LEFT JOIN pg_catalog.pg_class ic
                ON i.indrelid = ic.oid
            WHERE n.nspname = %s
        """, (self.schema,))
        print()
        for row in self.cursor:
            print(row)
        self.drop_views()
        self.drop_defaults()
        self.move_sequences()
        self.drop_original_table()
        self.rename_temp_table()

    def drop_views(self):
        for schema, viewname, viewdef in self.views:
            sql = 'DROP VIEW {}'.format(self.nameformat(viewname, schema))
            self.cursor.execute(sql)

    def drop_defaults(self):
        dropdefsql = 'ALTER TABLE {} ALTER COLUMN "{}" DROP DEFAULT'
        for col, default in self.defaults:
            self.cursor.execute(dropdefsql.format(self.nameformat(self.table), col))

    def move_sequences(self):
        seqownersql = 'ALTER SEQUENCE "{}" OWNED BY {}."{}"'
        for col, seq in self.sequences:
            self.cursor.execute(seqownersql.format(
                seq, self.nameformat(self.temp_name), col
            ))

    def drop_original_table(self):
        self.cursor.execute('DROP TABLE {}'.format(self.nameformat(self.table)))

    def rename_temp_table(self):
        template = 'ALTER {} {} RENAME TO {}'
        for obj_type, oldname, newname in self.rename:
            self.cursor.execute("""
                SELECT c.relkind, n.nspname, c.relname
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s
            """, (self.schema,))
            print()
            for row in self.cursor:
                print(row)
            print((obj_type, oldname, newname))
            self.cursor.execute(template.format(obj_type, oldname, newname))

    def create_views(self):
        viewsql = 'CREATE VIEW {} AS {}'
        for schema, viewname, viewdef in self.views:
            sql = viewsql.format(self.nameformat(viewname, schema), viewdef)
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

    def nameformat(self, name, schema=None):
        return '"{}"."{}"'.format(schema or self.schema, name)


class RenameReplace(Replace):
    "Subclass for renaming old table and recreating empty one like it"
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
        sql = 'ALTER %s "%s" RENAME TO "%s"'
        for objtype, temp, orig in self.rename:
            print(objtype, temp, orig)
            new_name = self.xform(orig)
            self.cursor.execute(sql % (objtype, orig, new_name))
        super(RenameReplace, self).rename_temp_table()


def rename_replace(connection, table, xform):
    with RenameReplace(connection, table, xform):
        pass
