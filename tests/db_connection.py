import contextlib
import os
import re

try:
    import psycopg
except ModuleNotFoundError:
    import psycopg2 as psycopg

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


connection_params = get_connection_params()


def connect(**kwargs):
    kw = connection_params.copy()
    kw.update(kwargs)
    conn = psycopg.connect(**kw)
    return conn


def create_db():
    "connect to test db"
    try:
        connect().close()
        return False
    except psycopg.OperationalError as exc:
        dbname = connection_params["dbname"]
        nosuch_db = 'database "%s" does not exist' % dbname
        if nosuch_db in str(exc):
            try:
                master = connect(dbname="postgres")
                master.rollback()
                master.autocommit = True
                cursor = master.cursor()
                cursor.execute("CREATE DATABASE %s" % dbname)
                cursor.close()
                master.close()
            except psycopg.Error as exc:
                message = (
                    "Unable to connect to or create test db %s.\nThe error is: %s"
                    % (dbname, exc)
                )
                raise RuntimeError(message)
            return True


def drop_db():
    "Drop test db"
    try:
        master = connect(dbname="postgres")
        master.rollback()
        master.autocommit = True
        cursor = master.cursor()
        cursor.execute("DROP DATABASE %s" % connection_params["dbname"])
        cursor.close()
        master.close()
    except psycopg.OperationalError:
        pass


def create_extensions(extensions):
    # always use psycopg2 connection to create extensions if necessary
    conn = connect()
    for extension in extensions:
        try:
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION {}".format(extension))
            conn.commit()
        except (
            psycopg.errors.DuplicateObject,
            psycopg.errors.UndefinedFile,  # postgres <= 14
            psycopg.errors.FeatureNotSupported,  # postgres >= 15
        ):
            conn.rollback()
    conn.close()


@contextlib.contextmanager
def conninfo(connection_params):
    conn = psycopg.connect(**connection_params)
    yield conn.info
    conn.close()
