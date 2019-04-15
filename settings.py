import os
import json
from collections import OrderedDict

# Base Path

BASE_DIR = os.path.abspath(os.getcwd())

# Loading rule mapping for all tasks
TABLE_MAPPINGS = json.loads(open(os.path.join(BASE_DIR, 'conf','table-mappings.json')).read(),object_pairs_hook=OrderedDict)


DMS_TASK_PARAMS = json.loads(open(os.path.join(BASE_DIR, 'conf','dms-task.json')).read(),object_pairs_hook=OrderedDict)

PARAMETERS = json.loads(open(os.path.join(BASE_DIR, 'conf','parameters.json')).read(),object_pairs_hook=OrderedDict)