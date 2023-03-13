import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def create_connection_in_memory():
    conn = None
    try:
        conn = sqlite3.connect(':memory:')
    except Error as e:
        print(e)

    return conn


def execute_sql(conn, sql):
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def add_table0(conn, name, surname, age, sex):
    sql = ''' INSERT INTO table0(name,surname,age,sex)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, (name, surname, age, sex))
    conn.commit()


def add_table1(conn, name, surname, age, sex):
    sql = ''' INSERT INTO table1(name,surname,age,sex)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, (name, surname, age, sex))
    conn.commit()


def select_all(conn, table):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")

    rows = cur.fetchall()

    for row in rows:
        print(row)


def select_where(conn, table, **query):
    condition = ' AND '.join([f"{k}='{v}'" for k, v in query.items()])
    sql = f"SELECT * FROM {table} WHERE {condition}"

    cur = conn.cursor()
    cur.execute(sql)

    rows = cur.fetchall()

    for row in rows:
        print(row)


def update(conn, table, id, **kwargs):
    set_values = ', '.join([f"{k}='{v}'" for k, v in kwargs.items()])
    sql = f"UPDATE {table} SET {set_values} WHERE id={id}"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def delete_where(conn, table, **kwargs):
    condition = ' AND '.join([f"{k}='{v}'" for k, v in kwargs.items()])
    sql = f"DELETE FROM {table} WHERE {condition}"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def delete_all(conn, table):
    sql = f"DELETE FROM {table}"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


if __name__ == '__main__':
    conn = create_connection(r"database.db")

    if conn is not None:
        execute_sql(conn, '''CREATE TABLE IF NOT EXISTS table0 (
                                       id integer PRIMARY KEY,
                                       name text NOT NULL,
                                       surname text NOT NULL,
                                       age integer NOT NULL,
                                       sex text NOT NULL
                                   );''')

        execute_sql(conn, '''CREATE TABLE IF NOT EXISTS table1 (
                                       id integer PRIMARY KEY,
                                       name text NOT NULL,
                                       surname text NOT NULL,
                                       age integer NOT NULL,
                                       sex text NOT NULL
                                   );''')

        add_table0(conn, 'Jan', 'Jankowski', 30, 'male')
        add_table0(conn, 'Janina', 'Jankowska', 28, 'female')

        add_table1(conn, 'Pawel', 'Nowak', 40, 'male')
        add_table1(conn, 'Ewa', 'Nowak', 38, 'female')

        # select_all(conn, 'table0')

        # select_where(conn, 'table0', name='Jan', surname='Jankowski')

        # update(conn, 'table0', 1, name='Michal')

        # delete_where(conn, 'table0', name='Michal')

        # delete_all(conn, 'table0')
        
        conn.close()

    else:
        print("Error while connecting to db")
