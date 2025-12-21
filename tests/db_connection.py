import os
import re

DB_HOST = os.getenv("POSTGRES_HOST")
IS_DSQL = bool(DB_HOST and re.match(r"^\w+\.dsql\.\w\w-\w+-[1-9]\.on\.aws$", DB_HOST))


def get_connection_params():
    if IS_DSQL:
        return dsql_connection_params()
    return {
        "dbname": os.getenv("POSTGRES_DB", "pgcopy_test"),
        "port": get_port(),
        "host": DB_HOST,
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def dsql_connection_params():
    import boto3

    dsql = boto3.client("dsql")
    admin_token = dsql.generate_db_connect_admin_auth_token(
        Hostname=DB_HOST,
        ExpiresIn=300,
    )
    return {
        "user": "admin",
        "host": DB_HOST,
        "password": admin_token,
        "dbname": "postgres",
        "sslmode": "require",
    }


def get_port():
    # this would be much more straightforward if tox-docker would release
    # recent updates https://github.com/tox-dev/tox-docker/pull/167
    if os.getenv("TOX_ENV_NAME"):
        search_pattern = re.compile(r"PG\w+_5432_TCP_PORT")
        for name, val in os.environ.items():
            if search_pattern.fullmatch(name):
                return int(val)
    return int(os.getenv("POSTGRES_PORT", "5432"))
