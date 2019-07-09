def get_types(conn, schema, table):
    # for arrays:
    # typname has '_' prefix
    # attndims > 0
    # typcategory is 'A'
    # typelem is typid of individual elem (otherwise zero)
    query = """
            SELECT
                    a.attname,
                    t.typname,
                    a.atttypmod,
                    a.attnotnull,
                    t.typelem
            FROM
                    pg_class c
                    JOIN pg_attribute a ON a.attrelid = c.oid
                    JOIN pg_type t ON a.atttypid = t.oid
                    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s and relname = %s and attnum > 0
            ORDER BY c.relname, a.attnum;
            """
    cursor = conn.cursor()
    cursor.execute(query, (schema, table,))
    type_dict = {}
    for rec in cursor:
        type_dict[rec[0]] = rec[1:]
    return type_dict

