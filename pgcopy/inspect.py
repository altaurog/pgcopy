from psycopg2.extras import NamedTupleCursor


def get_types(conn, schema, table):
    # for arrays:
    # typname has '_' prefix
    # attndims > 0
    # typcategory is 'A'
    # typelem is typid of individual elem (otherwise zero)
    query = """
            SELECT
                    a.attname,
                    t.typcategory AS type_category,
                    COALESCE(et.typname, t.typname) AS type_name,
                    a.atttypmod AS type_mod,
                    a.attnotnull AS not_null,
                    t.typelem
            FROM
                    pg_catalog.pg_class c
                    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                    JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
                    LEFT JOIN pg_catalog.pg_type et ON t.typelem = et.oid
                    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s and relname = %s and attnum > 0
            ORDER BY c.relname, a.attnum;
            """
    cursor = conn.cursor(cursor_factory=NamedTupleCursor)
    cursor.execute(query, (schema, table))
    return {r.attname: r for r in cursor}
