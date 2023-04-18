import logging
import sqlite3
from typing import Dict

logging.basicConfig(level=logging.INFO)

def create_db(db_name: str) -> sqlite3.Connection:
    """
    This function creates a sqlite3 db with the supplied name
    :param db_name: Name of DB
    :return: Connection object
    """
    try:
        con = sqlite3.connect(db_name)
        logging.info("Created database connection.")
        return con
    except sqlite3.Error as e:
        logging.error(f"Error creating database connection: {e}")
        raise


def create_table(con: sqlite3.Connection, table_name: str, sql: str, drop_first: bool = False) -> None:
    """
    This function creates table in the above database
    :param con: Connection object
    :param table_name: name of the table
    :param sql: The sql statement to create the table with the columns
    :param drop_first: A boolean value to drop the table and then create. The default value is false
    :return: None
    """
    try:
        cursor = con.cursor()
        if drop_first:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(sql)
        con.commit()
        logging.info(f"Table {table_name} created successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error creating table {table_name}: {e}")
        raise


def sql_insert(con: sqlite3.Connection, table: str, data: Dict[str, str]) -> None:
    """
    This function is to insert data into the supplied table and data
    :param con: Connection object
    :param table: name of the table
    :param data: supplied fields of data
    :return: None
    """
    try:
        cursor = con.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, tuple(data.values()))
        con.commit()
        logging.info(f"Data inserted into {table} successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting data into {table}: {e}")
        raise


def round_off(con: sqlite3.Connection, table: str, columns: list[str]) -> None:
    try:
        cursor = con.cursor()
        query = f"UPDATE {table} SET "
        for column in columns:
            query += f"{column} = ROUND({column}, 2),"
        query = query[:-1] + ";"
        cursor.execute(query)
        con.commit()
        logging.info(f"Columns rounded off successfully.")
    except sqlite3.Error as e:
        logging.error(f"Columns couldn't be rounded off: {e}")
        raise

