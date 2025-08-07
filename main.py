# Entry point for the Data Quality Dashboard CLI

import argparse
import logging
import sqlite3
import pathlib
from scripts.cleaner import clean_folder

parser = argparse.ArgumentParser()

parser.add_argument("--folder", required=True, help="Path where folder lives")
parser.add_argument("--logfile")
parser.add_argument("-dryrun")
parser.add_argument("--verbose")

args = parser.parse_args()

log_level = logging.DEBUG if args.verbose else logging.INFO
logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s %(message)s]",
    handlers=[
        logging.FileHandler(args.logfile if args.logfile else "activity.log"),
        logging.StreamHandler()
    ]
)

#1.) Scans a folder of .csv and .json files
folderPath = pathlib.Path(args.folder)

if not folderPath.exists():
    logging.error(f"The folder you inputed is not a folder: {args.folder}")
    exit(1)




#2.) Cleans and stores the data in SQLite
#3.) Analyzes the data for errors, duplicates, and schema mismatches
#4.) Generates a report of potential quality issues
#5.) Can be run manually or automatically (on EC2)

