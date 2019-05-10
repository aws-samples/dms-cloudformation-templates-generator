#
# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#

import os
import json
from collections import OrderedDict

# Base Path

BASE_DIR = os.path.abspath(os.getcwd())

# Loading rule mapping for all tasks
TABLE_MAPPINGS = json.loads(open(os.path.join(BASE_DIR, 'conf', 'table-mappings.json')).read(), object_pairs_hook=OrderedDict)


DMS_TASK_PARAMS = json.loads(open(os.path.join(BASE_DIR, 'conf', 'dms-task.json')).read(), object_pairs_hook=OrderedDict)

PARAMETERS = json.loads(open(os.path.join(BASE_DIR, 'conf', 'parameters.json')).read(), object_pairs_hook=OrderedDict)