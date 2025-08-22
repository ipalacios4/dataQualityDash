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
                    if not dryrun:
                        cleaned_csv.append(row)
                else:
                    csvStats['invalid'] += 1
                    logging.debug(f"Row not accepted: {row}")
    except csv.Error:
        csvStats['fileError'] += 1
        logging.error(f"Error loading csv file: {file.name}")   


def process_json_file(file, jsonStats, dryrun, cleaned_json):
    jsonKeys = ["user_id", "action", "timestamp"] # temporary keys to search for rn
    try:
        with open(file, 'r') as inputFile:
            jsonfile = json.load(inputFile)
            # later we will generalize the code for now just use the keys u know

            for data in jsonfile: # is data here a dictionary?
                if all(data.get(field) not in [None,""] for field in jsonKeys): 
                    jsonStats['valid'] += 1
                    logging.debug(f"Valid JSON record {data}")
                    if not dryrun:
                        cleaned_json.append(data)
                    
                else:
                    jsonStats['invalid'] += 1
                    logging.debug(f"Invlaid JSON record: {data}")
    except json.JSONDecodeError:
        jsonStats['fileError'] += 1
        logging.error(f"Error loading JSON file {file.name}")


def clean_folder(path: pathlib.Path, dryrun: bool):
    
    fileSkipped = 0 # the file was skipped
    fileProcessed = 0 # file was touched
    validRows = 0 
    totalRows = 0


    #Processed mean file was touched
    #Valid = row was collected with no error
    #invalid = row had data missing and is skipped
    csvStats = {'processed':0, 'valid':0, 'invalid': 0, 'fileError': 0}
    jsonStats = {'processed':0, 'valid':0, 'invalid': 0, 'fileError': 0}

    cleaned_csv = []
    cleaned_json = []

    logging.debug(f"clean_folder function called. Path:{path}, dryrun:{dryrun}")
    for file in path.iterdir():
        logging.info(f"current file: {file.name}")
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

    fileProcessed = csvStats['processed'] + jsonStats['processed']

    if dryrun:
        logging.info(f"Total number of processed files: {fileProcessed}")
        logging.info(f"Number of unsupported files: {fileSkipped}")
        logging.info(f"Number of malformed files:\nCSV:{csvStats['fileError']}\nJSON:{jsonStats['fileError']}")

    else: 
        return {
            "CSV" : cleaned_csv,
            "JSON" : cleaned_json
        }
 