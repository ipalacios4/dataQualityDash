# Will contain functions to insert data into SQLite
import sqlite3
import pathlib
from schema import user_schema, activity_schema
import logging 



# db_path is the path to main db no the tables. Tables live within db
def connect_db(db_path: pathlib.Path, dryrun: bool):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    return conn, cursor

#here is where we will create the tables in the db
def create_schema(cursor, logger):
    # Hard coded schemas (We will adjust it later to be dynamic)


    columns = ", ".join(f"{col} {type_}" for col, type_ in user_schema.items())
    create_table = f"CREATE TABLE IF NOT EXISTS users ({columns})"
    logger.info("Table being created for users")
    cursor.execute(create_table)



    columns = ", ".join(f"{col} {type_}" for col, type_ in activity_schema.items())
    create_table = f"CREATE TABLE IF NOT EXISTS activity ({columns})"
    logger.info("Table being created for activity")
    cursor.execute(create_table)



def prepare_inserts(cleaned, schema, logger):
    columns = list(schema.keys())
    insert_rows = [tuple(row[field]for field in columns) for row in cleaned]

    return columns, insert_rows

#make sure to pass columns from prepare_inserts 
def insert_records(cursor, table, columns, values, dryrun, logger):
    placeholders = ", ".join(["?"] * len(columns))

    insert_sql = f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})"
    logger.debug(f"Preparing SQL: {insert_sql}")
    logger.debug(f"Insert into table: {table}, Rows: {len(values)}")

    if dryrun:
        logger.info(f"Would insert {len(values)} rows into {table}")
    
    try:
        cursor.executemany(insert_sql, values)
        logger.info(f"Inserted {len(values)} rows into {table}")
    except Exception as err:
        logger.error(f"Failed to insert into {table}: {err}")

def commit_or_rollback(conn, dryrun, logger):
    if dryrun:
        logger.info("Skipping commiting")
    try:
        conn.commit()
        logger.info("Changes commited to database")
    except Exception as err:
        logger.error(f"Commit failed - rolling back. Error {err}")
        conn.rollback()
        logger.info("Rolled back uncommited changes")



def close(conn, logger):
    try:
        conn.close()
        logger.info("Database connection closed.")
    except Exception as err:
        logger.warning(f"Failed to close DB connection: {err}")
