
import sqlite3
from sqlite3 import Error




def create_connection(db_file):
    """ Create a database connection to a SQLite db
    : param db_file: database file
    return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    
    return conn


def create_table(conn, create_table_sql):
    """ Create a table from a sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return :
    """
    try: 
        curs = conn.cursor()
        curs.execute(create_table_sql)
    except Error as e:
        print(e)


def main():
    db_file = r"data\countries.db"

    sql_drop_table = """ DROP TABLE countries"""
    sql_create_countries_table = """ CREATE TABLE IF NOT EXISTS countries (
                                    id integer PRIMARY KEY,
                                    Country text UNIQUE NOT NULL,
                                    Region text,
                                    Population integer,
                                    Area integer,
                                    Pop_Density real,
                                    Coastline_Ratio real,
                                    Net_Migration real,
                                    Infant_Mortality real,
                                    GDP_Per_Capita real,
                                    Literacy real,
                                    Phones real,
                                    Arable real,
                                    Crops real,
                                    Other real,
                                    Climate real,
                                    Birthrate real,
                                    Deathrate real,
                                    Agriculture real,
                                    Industry real,
                                    Service real 
                                    ) """   

    #Create a database connection
    conn = create_connection(db_file)

    #Create table
    if conn is not None:
        curs = conn.cursor()
        #remove following line to avoid resetting the table
        #curs.execute(sql_drop_table)
        conn.commit()
        create_table(conn, sql_create_countries_table)
        conn.close()
    else:
        print("Error, cannot create database connection.")

if __name__ == "__main__":
    main()

