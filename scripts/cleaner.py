# Will contain functions to clean and validate input data
import csv
import json
import logging
import pathlib

# NEED TO APPLY DRYRUN TO YOUR HELPER FUNCTION

def process_csv_file(file, csvStats, dryrun, cleaned_csv):
    try:
        with open(file, 'r') as inputFile:
            reader = csv.DictReader(inputFile)
            header = list(next(reader).keys())
            for row in reader:
                if all(row[col].strip() for col in header):
                    csvStats['valid'] += 1
                    logging.debug(f"Valid row: {row}")
                    cleaned_csv.append(row)
                else:
                    csvStats['invalid'] += 1
                    logging.debug(f"Row not accepted: {row}")
    except csv.Error:
        logging.error(f"Error loading csv file: {file.name}")   


def process_json_file(file, jsonStats, dryrun, cleaned_json):
    try:
        with open(file, 'r') as inputFile:
            jsonfile = json.load(file)
            keys = list(jsonfile.keys())

            for data in jsonfile: # is data here a dictionary?
                if all(data.get(field) not in [None,""] for field in jsonfile): 
                    jsonStats['valid'] += 1
                    logging.debug(f"Valid JSON record {data}")
                    cleaned_json.append(data)
                    
                else:
                    jsonStats['invalid'] += 1
                    logging.debug(f"Invlaid JSON record: {data}")
    except json.JSONDecodeError:
        logging.error(f"Error loading JSON file {file.name}")


def clean_folder(path: pathlib.Path, dryrun: bool):
    fileSkipped = 0
    fileProcessed = 0
    validRows = 0
    totalRows = 0

    csvStats = {'processed':0, 'valid':0, 'invalid': 0}
    jsonStats = {'processed':0, 'valid':0, 'invalid': 0}

    cleaned_csv = []
    cleaned_json = []

    logging.debug(f"clean_folder function called. Path:{path}, dryrun:{dryrun}")
    for file in path.iterdir():
        match file.suffix.lstrip("."):
            case "csv":
                logging.debug(f"File is a csv: {file.name}")
                process_csv_file(file, csvStats, dryrun, cleaned_csv)
                csvStats['processed'] += 1

            case "json":
                logging.debug(f"File is a json: {file.name}")
                process_json_file(file, jsonStats, dryrun, cleaned_json)
                jsonStats['processed'] += 1
            
            case _:
                logging.warning(f"Skipping unsupported file type: {file.name}")
                fileSkipped += 1


