import psycopg2
import configparser
from sql_queries import drop_table_queries


def create_database():
    """Creates database.

    Returns:
        cur (psycopg2.cursor): The database cursor
        conn (psycopg2.connection): The database connection
    """
    # connect to default database
    config = configparser.ConfigParser()
    config.read("dwh.cfg")
    try:
        conn = psycopg2.connect(
            "host={} dbname={} user={} password={}".format(config["CLUSTER"]["HOST"], config["CLUSTER"]["DB_NAME"],
                                                           config["CLUSTER"]["DB_USER"],
                                                           config["CLUSTER"]["DB_PASSWORD"]))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    conn.set_session(autocommit=True)

    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS " + config["IMMIGRATION"]["DB_NAME"])
    except psycopg2.Error as e:
        print("Error: Could not drop Database")
        print(e)

    try:
        cur.execute("CREATE DATABASE " + config["IMMIGRATION"]["DB_NAME"] + " WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print("Error: Could not create Database")
        print(e)

    # close connection to default database
    conn.close()

    # connect to sparkify database
    try:
        conn = psycopg2.connect(
            "host={} dbname={} user={} password={}".format(config["CLUSTER"]["HOST"], config["IMMIGRATION"]["DB_NAME"],
                                                           config["CLUSTER"]["DB_USER"],
                                                           config["CLUSTER"]["DB_PASSWORD"]))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    return cur, conn


def drop_tables(cur, conn):
    """Drops all tables defined in `sql_queries.drop_table_queries`.

    Args:
        cur (psycopg2.cursor): A database cursor
        conn (psycopg2.connection): A database connection
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Could not drop table from query: {}".format(query))
            print(e)


def create_tables(cur, conn):
    """Creates all tables defined in `sql_queries.create_table_queries`.

    Args:
        cur (psycopg2.cursor): A database cursor
        conn (psycopg2.connection): A database connection
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Could not create table from query: {}".format(query))
            print(e)


def main():
    """
    Performing following actions:
    1) Creating database.
    2) Droping tables if exists.
    3) Creating tables.
    4) Closing cursor and connection.
    """

    cur, conn = create_database()

    drop_tables(cur, conn)
    # create_tables(cur, conn)

    cur.close();
    conn.close()


if __name__ == "__main__":
    main()