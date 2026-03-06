import mysql.connector
from config.db_config import DB_CONFIG


def get_connection():
    """
    Creates and returns a MySQL DB connection
    """
    return mysql.connector.connect(**DB_CONFIG)


def fetch_all(query, params=None):
    """
    Executes a SELECT query and returns all rows
    """
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query, params or ())
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


def fetch_one(query, params=None):
    """
    Executes a SELECT query and returns one row
    """
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query, params or ())
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result


def execute(query, params=None):
    """
    Executes INSERT / UPDATE / DELETE
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query, params or ())
    conn.commit()
    cursor.close()
    conn.close()