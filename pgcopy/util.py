import logging
import random
import re
import string
from datetime import datetime

from pytz import UTC

logger = logging.getLogger(__package__)


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


def idre(name):
    return re.compile(r'\b%s\b' % (re.escape(name)))


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
    def __init__(self, connection, table, schema='public'):
        self.cursor = connection.cursor()
        self.uid = uid()
        self.schema = schema
        self.table = table
        self.name_re = idre(table)
        self.temp_name = self.newname()
        self.rename_temp = [(
            'TABLE',
            '{}"."{}'.format(self.schema, self.temp_name),
            table
        )]
        self.rename_orig = [(
            'TABLE',
            '{}"."{}'.format(self.schema, table),
            table
        )]
        self.objtype_requiring_rename_with_schema = (
            'SEQUENCE',
        )
        self.inspect()

    def execute_sql(self, sql, *args, **kwargs):
        logger.debug('SQL to be executed: {}, {}, {}'.format(sql, args, kwargs))
        self.cursor.execute(sql, *args, **kwargs)

    def __enter__(self):
        # Create temp table with defaults
        self.create_temp()
        self.create_defaults()

        return self.temp_name

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            # Swap temp to final
            self.swap()
        self.cursor.close()

    def inspect(self):
        defquery = """
            SELECT attname, pg_get_expr(adbin, adrelid)
            FROM pg_attribute
            JOIN pg_attrdef ON attrelid = adrelid AND adnum = attnum
            WHERE adnum > 0
            AND attrelid = %s::regclass;
            """
        self.execute_sql(defquery, ('.'.join([self.schema, self.table]),))
        self.defaults = self.cursor.fetchall()
        seqquery = """
            SELECT attname, relname FROM pg_class
            JOIN pg_depend ON (objid = pg_class.oid)
            JOIN pg_attribute ON (attnum=refobjsubid AND attrelid=refobjid)
            WHERE relkind = 'S'
            AND refobjid = %s::regclass
            """
        self.execute_sql(seqquery, ('.'.join([self.schema, self.table]),))
        self.sequences = self.cursor.fetchall()
        attquery = """
            SELECT attname
            FROM pg_catalog.pg_attribute
            WHERE attrelid = %s::regclass
            AND attnum > 0 AND attnotnull
            """
        self.execute_sql(attquery, ('.'.join([self.schema, self.table]),))
        self.notnull = [an for (an,) in self.cursor]
        # primary key is recreated as a constraint,
        # but all other unique constraints are only
        # recreated as unique index
        conquery = """
            SELECT DISTINCT contype, conname, pg_catalog.pg_get_constraintdef(oid)
            FROM pg_catalog.pg_constraint
            WHERE conrelid = %s::regclass AND contype != 'u'
            """
        self.execute_sql(conquery, ('.'.join([self.schema, self.table]),))
        self.constraints = self.cursor.fetchall()
        indquery = """
            SELECT c.relname, pg_catalog.pg_get_indexdef(i.indexrelid)
            FROM pg_catalog.pg_index i
            JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
            WHERE NOT indisprimary
            AND indrelid = %s::regclass
            """
        self.execute_sql(indquery, ('.'.join([self.schema, self.table]),))
        self.indices = self.cursor.fetchall()
        trigquery = """
            SELECT tgname, pg_catalog.pg_get_triggerdef(oid)
            FROM pg_catalog.pg_trigger
            WHERE tgconstraint = 0
            AND tgrelid=%s::regclass
            """
        self.execute_sql(trigquery, ('.'.join([self.schema, self.table]),))
        self.triggers = self.cursor.fetchall()
        viewquery = """
            SELECT DISTINCT c.relname, pg_get_viewdef(r.ev_class)
            FROM pg_rewrite r
            JOIN pg_depend d ON d.objid = r.oid
            JOIN pg_class c ON c.oid = r.ev_class
            WHERE d.refobjid = %s::regclass;
            """
        self.execute_sql(viewquery, ('.'.join([self.schema, self.table]),))
        self.views = self.cursor.fetchall()

    def create_temp(self):
        create = 'CREATE TABLE "%s" AS TABLE "%s" WITH NO DATA'
        self.execute_sql(create % (
            '{}"."{}'.format(self.schema, self.temp_name),
            '{}"."{}'.format(self.schema, self.table)
        ))

    def create_defaults(self):
        defsql = 'ALTER TABLE "%s" ALTER COLUMN "%s" SET DEFAULT %s'
        for col, default in self.defaults:
            self.execute_sql(defsql % (
                '{}"."{}'.format(self.schema, self.temp_name),
                col,
                default
            ))

    def create_notnull(self):
        nnsql = 'ALTER TABLE "%s" ALTER COLUMN "%s" SET NOT NULL'
        for col in self.notnull:
            self.execute_sql(nnsql % (
                '{}"."{}'.format(self.schema, self.temp_name),
                col
            ))

    def create_constraints(self):
        consql = 'ALTER TABLE "%s" ADD CONSTRAINT "%s" %s'
        for i, (contype, conname, condef) in enumerate(self.constraints):
            newname = self.newname('con', i)
            self.execute_sql(consql % (
                '{}"."{}'.format(self.schema, self.temp_name),
                newname,
                condef
            ))
            if 'p' == contype:
                self.rename_temp.append((
                    'INDEX',
                    '{}"."{}'.format(self.schema, newname),
                    conname
                ))
                self.rename_orig.append((
                    'INDEX',
                    '{}"."{}'.format(self.schema, conname),
                    conname
                ))

    def create_indices(self):
        for i, (oldidxname, indexsql) in enumerate(self.indices):
            newidxname = self.newname('idx', i)
            newsql = self.sqlrename(indexsql, oldidxname, newidxname)
            self.execute_sql(newsql)
            self.rename_temp.append((
                'INDEX',
                '{}"."{}'.format(self.schema, newidxname),
                oldidxname
            ))
            self.rename_orig.append((
                'INDEX',
                '{}"."{}'.format(self.schema, oldidxname),
                oldidxname
            ))

    def create_triggers(self):
        for i, (oldtrigname, trigsql) in enumerate(self.triggers):
            newtrigname = self.newname('tg', i)
            newsql = self.sqlrename(trigsql, oldtrigname, newtrigname)
            self.execute_sql(newsql)
            # ALTER TRIGGER name ON table_name RENAME TO new_name
            # CREATE TRIGGER test_rd_tg_00_eient BEFORE INSERT OR UPDATE ON rd.test_rd_eient FOR EACH ROW EXECUTE PROCEDURE rd.alter_type_trigger() () {}
            self.rename_temp.append((
                'TRIGGER',
                '%s" ON "%s' % (
                    newtrigname,
                    '{}"."{}'.format(self.schema, self.table)
                ),
                oldtrigname
            ))
            self.rename_orig.append((
                'TRIGGER',
                '%s" ON "%s' % (
                    oldtrigname,
                    '{}"."{}'.format(self.schema, self.table)
                ),
                oldtrigname
            ))

    def swap(self):
        # Create constraints, indices and triggers on temp table
        self.create_notnull()
        self.create_constraints()
        self.create_indices()
        self.create_triggers()
        # Drop original and views
        self.drop_views()
        self.drop_defaults()  # FIXME: needed? why?
        # Move sequence to temp table
        self.move_sequences()
        self.drop_original_table()
        # Rename everything temp to final
        self.rename_temp_table()
        # Re-create the previously dropped views on the new table (new reference)
        self.create_views()

    def drop_views(self):
        for view, viewdef in self.views:
            self.execute_sql('DROP VIEW "%s"' % (
                '{}"."{}'.format(self.schema, view)
            ))

    def drop_defaults(self):
        dropdefsql = 'ALTER TABLE "%s" ALTER COLUMN "%s" DROP DEFAULT'
        for col, default in self.defaults:
            self.execute_sql(dropdefsql % (
                '{}"."{}'.format(self.schema, self.table),
                col
            ))

    def move_sequences(self):
        seqownersql = 'ALTER SEQUENCE "%s" OWNED BY "%s"."%s"'
        for col, seq in self.sequences:
            self.execute_sql(seqownersql % (
                '{}"."{}'.format(self.schema, seq),
                '{}"."{}'.format(self.schema, self.temp_name),
                col
            ))

    def drop_original_table(self):
        self.execute_sql('DROP TABLE "%s"' % (
            '{}"."{}'.format(self.schema, self.table)
        ))

    def rename_temp_table(self):
        sql = 'ALTER %s "%s" RENAME TO "%s"'
        for rename in self.rename_temp:
            self.execute_sql(sql % rename)

    def create_views(self):
        viewsql = 'CREATE VIEW "%s" AS %s'
        for view in self.views:
            sql = viewsql % (
                '{}"."{}'.format(self.schema, view)
            )
            self.execute_sql(sql)

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
    "Subclass for renaming old table and recreating empty one like it"
    def __init__(self, connection, table, xform, schema='public'):
        """
        xform must be a function which translates old
        names to new ones, used on tables & pk constraints
        """
        super(RenameReplace, self).__init__(connection, table, schema)
        self.xform = xform

    def drop_original_table(self):
        pass

    def rename_temp_table(self):
        logger.debug('Rename replace: rename orig')
        sql = 'ALTER %s "%s" RENAME TO "%s"'
        for objtype, orig, to_rename in reversed(self.rename_orig):  # reverse because we can't rename the table before what depends on it
            new_name = self.xform(to_rename)
            self.execute_sql(sql % (
                objtype,
                orig,
                new_name
            ))
        logger.debug('Rename replace: rename temp')
        super(RenameReplace, self).rename_temp_table()


def rename_replace(connection, table, xform):
    with RenameReplace(connection, table, xform):
        pass
