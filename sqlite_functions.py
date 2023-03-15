import sqlite3
from sqlite3 import Error


def create_db(db_name):
    """
    This function creates a sqlite3 db with the supplied name
    :param db_name: Name of DB
    :return:
    """
    try:
        con = sqlite3.connect(db_name)
        return con
    except Error:
        print(Error)


def create_table(con, table_name, sql, drop_first: bool = False):
    """
    This function creates table in the above database
    :param con:
    :param table_name: name of the table
    :param sql: The sql statement to create the table with the columns
    :param drop_first: A boolean value to drop the table and then create. The default value is false
    :return:
    """
    try:
        cursorObj = con.cursor()
        if drop_first:
            cursorObj.execute('''DROP TABLE IF EXISTS {}'''.format(table_name))
        cursorObj.execute(sql)
        con.commit()
    except Error:
        print(Error)


def sql_insert(con, table, data):
    """
    This function is to insert data into the supplied table and data
    :param con:
    :param table: name of the table
    :param data: supplied fields of data
    :return:
    """
    try:
        cursorObj = con.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        cursorObj.execute(query, tuple(data.values()))
        con.commit()
    except Error:
        print(Error)
