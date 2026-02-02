import fnmatch
import json
import os
from types import SimpleNamespace
from fkie_iop_json_connector.logger import MyLogger

JSON_SCHEMES = {}

def init_schemes(schemesPath='', loglevel='info'):
    global JSON_SCHEMES
    JSON_SCHEMES.clear()
    logger = MyLogger('schemes', loglevel=loglevel)
    # load json message schemes
    jsonFiles = set()
    schemes_dir = schemesPath
    if not schemes_dir:
        schemes_dir = os.path.dirname(os.path.abspath(__file__))
    logger.info(f"Read JSON schemes message from {schemes_dir}")
    for root, _dirnames, filenames in os.walk(schemes_dir):
        for filename in fnmatch.filter(filenames, '*.json'):
            jsonFile = os.path.join(root, filename)
            jsonFiles.add(os.path.join(root, jsonFile))
    for jsonFile in jsonFiles:
        with open(jsonFile, 'r') as jFile:
            schema = json.load(
                jFile, object_hook=lambda d: SimpleNamespace(**d))
            if schema.title:
                if schema.messageId in JSON_SCHEMES:
                    JSON_SCHEMES[schema.messageId].append(schema)
                else:
                    JSON_SCHEMES[schema.messageId] = [schema]
    schemas_count = 0
    schemas_double_count = 0
    for key, schemas in JSON_SCHEMES.items():
        schemas_count += len(schemas)
        if len(schemas) > 1:
            schemas_double_count += 1
    logger.info(f"{schemas_count} message schemes found.")
    if schemas_double_count > 0:
        logger.warning(f" > {schemas_double_count} ids have multiple schemas!")
