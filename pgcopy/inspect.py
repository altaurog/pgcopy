def get_types(conn, table):
    query = """
            SELECT
                    a.attname,
                    t.typname,
                    a.atttypmod,
                    a.attnotnull
            FROM
                    pg_class c
                    JOIN pg_attribute a ON a.attrelid = c.oid
                    JOIN pg_type t ON a.atttypid = t.oid
            WHERE relname = %s and attnum > 0
            ORDER BY c.relname, a.attnum;
            """
    cursor = conn.cursor()
    cursor.execute(query, (table,))
    type_dict = {}
    for rec in cursor:
        type_dict[rec[0]] = rec[1:]
    return type_dict

