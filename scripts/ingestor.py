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



def prepare_inserts(cleaned, schema):
    columns = list(schema.keys())
    insert_rows = [tuple(row[field]for field in columns) for row in cleaned]

    return columns, insert_rows

#make sure to pass columns from prepare_inserts 
def insert_records(cursor, table, columns, values, dryrun, logger):
    placeholders = ", ".join(["?"] * len(columns))

    insert_sql = f"INSERT OR IGNORE INTO {table} ({", ".join(columns)}) VALUES ({placeholders})"
    logger.debug(f"Preparing SQL: {insert_sql}")
    logger.debug(f"Insert into table: {table}, Rows: {len(values)}")

    if dryrun:
        logger.info(f"Would insert {len(values)} rows into {table}")
    
    try:
        cursor.executemany(insert_sql, values)
        logger.info(f"Inserted {len(values)} rows into {table}")
    except Exception as err:
        logger.error(f"Failed to insert into {table}: {err}")
        logger.error(columns)

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


def ingestor_runner(clean_data, db_path, dryrun, logger):
    conn, cursor = connect_db(db_path, dryrun) #returns conn and cursor
    create_schema(cursor, logger)
    
    # Prepare and insert for CSV
    columns, insert_rows = prepare_inserts(clean_data["CSV"], user_schema) #returns columns and cleaned tuples
    insert_records(cursor, table='users', columns=columns, values=insert_rows, dryrun=dryrun, logger=logger)

    # Prepare and insert for JSON
    columns, insert_rows = prepare_inserts(clean_data["JSON"], activity_schema)
    insert_records(cursor, 'activity', columns, insert_rows, dryrun, logger)

    commit_or_rollback(conn, dryrun, logger)
    close(conn, logger)


    #the way this should work is connect
    # then create_schema for both csv and json x2 calls
    # prepare_inserts x2 call for each file type
    # insert_records x2 call for each type
    # commit or rollback will be on call and so will close be