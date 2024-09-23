import fnmatch
import json
import os
from types import SimpleNamespace
from fkie_iop_json_connector.logger import MyLogger

JSON_SCHEMES = {}

def init_schemes(schemesPath='', loglevel='info'):
    global JSON_SCHEMES
    logger = MyLogger('schemes', loglevel)
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
                JSON_SCHEMES[schema.messageId] = schema
    logger.info(f"{len(JSON_SCHEMES)} message schemes found")
