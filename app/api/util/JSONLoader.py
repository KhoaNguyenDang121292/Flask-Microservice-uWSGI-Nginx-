import json


def loadJSONFile(file_path):
    with open(file_path) as f:
        data = json.load(f)
    return data
