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

import os, argparse, json, copy, sys, re
from collections import OrderedDict

import xlrd

# Base Path
BASE_DIR = os.path.abspath(os.getcwd())

# Loading rule mappings for all tasks
TABLE_MAPPINGS_TEMPLATE_FILE = os.path.join(BASE_DIR, "conf", "table-mappings.json")
JSON_TABLE_MAPPINGS = json.loads(open(TABLE_MAPPINGS_TEMPLATE_FILE).read(), object_pairs_hook=OrderedDict)

def create_task(name,
                schema_name,
                description,
                task_sheet,
                migration_type = "full-load",
                source_arn = None,
                target_arn = None,
                replica_arn = None,
                tags = None,
                cdc_start_time = None):
    i = 1 # counter for rule-id
    num_cols, task_tables= task_sheet.ncols, task_sheet.nrows
    task_name = re.sub(r"[^\w]",'', name)
    op_template = copy.deepcopy(JSON_DEFAULT_TEMPLATE_FILE)

    # Add endpoints and replication instance as perametes

    if source_arn:  # Replace Source ARN in Parameters in task dms cloudformation
        op_template["Parameters"]["SourceEndpoint"]["Default"] = source_arn
    if target_arn:  # Replace Target ARN in Parameters in task dms cloudformation
        op_template["Parameters"]["TargetEndpoint"]["Default"] = target_arn
    if replica_arn: # Replace ReplicaARN in Parameters in task dms cloudformation
        op_template["Parameters"]["ReplicationServerARN"]["Default"] = replica_arn

    op_template["Resources"][task_name] = op_template["Resources"].pop("TaskNameFromConfig")
    op_template["Resources"][task_name]["Properties"]["ReplicationTaskIdentifier"] = name
    op_template["Resources"][task_name]["Properties"]["MigrationType"] = migration_type
    op_template["Description"] = description

    rules = {"rules" : []}
    selection_json = JSON_TABLE_MAPPINGS["selection"]
    transformation_default_json = JSON_TABLE_MAPPINGS["transformation_default"]
    for row_idx in range(1, task_tables):
        table_name = task_sheet.cell(row_idx, 1).value
        exclude_columns_list = task_sheet.cell(row_idx, 2).value.split(',')
        exclude_columns = list()
        for col in exclude_columns_list:
            exclude_columns.append(str(col).strip())
        #exclude_columns = map(str.strip, exclude_columns_list)
        table_json_op = copy.deepcopy(selection_json)
        table_json_op["rule-id"] = i
        table_json_op["rule-name"] = i
        table_json_op["object-locator"]["schema-name"] = schema_name
        table_json_op["object-locator"]["table-name"] = table_name
        rules["rules"].append(table_json_op)
        i += 1

        for exclude_column in exclude_columns: # Adding Exclude Colllumns Rule to task if exists
            if not exclude_column:
                continue
            exc_json_op = copy.deepcopy(transformation_default_json)
            exc_json_op["rule-type"] = "transformation"
            exc_json_op["rule-id"] = i
            exc_json_op["rule-name"] = i
            exc_json_op["rule-target"] = "column"
            exc_json_op["object-locator"]["schema-name"] = schema_name
            exc_json_op["object-locator"]["table-name"] = table_name
            exc_json_op["object-locator"]["column-name"] = exclude_column
            exc_json_op["rule-action"] = "remove-column"
            rules["rules"].append(exc_json_op)
            i += 1

    for trans_rule in JSON_TABLE_MAPPINGS["transformation"]: # Adding transformation rules to task
        trans_rule["rule-id"] = i
        trans_rule["rule-name"] = i
        trans_rule["object-locator"]["schema-name"] = schema_name
        rules["rules"].append(trans_rule)
        i += 1

    op_template["Resources"][task_name]["Properties"]["TableMappings"] = json.dumps(rules).replace('"', '\"')

    # Add Common tags if exist
    if tags and len(tags) > 0 : op_template["Resources"][task_name]["Properties"]["Tags"] = tags

    # Add CDCStartTime if applicable
    if cdc_start_time : op_template["Resources"][task_name]["Properties"]["CdcStartTime"] = cdc_start_time

    if not os.path.exists(os.path.join(BASE_DIR, "output")): # Creating output folder if not exists
        os.makedirs(os.path.join(BASE_DIR, "output"))

    with open(os.path.join(BASE_DIR, "output",  name+".template"), "w") as writefile:
        json.dump(op_template, writefile, indent=4)
        print("Created Task Template for %s"%name)

def form_tag(key,value):
    tag = dict()
    tag['Key'] = key
    tag['Value'] = value
    return tag

def get_cell_str(cell):
    if cell.ctype == xlrd.XL_CELL_TEXT: return cell.value
    elif cell.value == int(cell.value): return int(cell.value)
    else: return str(ckey.value)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DMS Task options')
    parser.add_argument('--path', required=True, help='Source Path xls')
    parser.add_argument('--type', required=True, choices=['cdc','full-load','full-load-and-cdc'], help='cdc/full-load/full-load-and-cdc')
    args = parser.parse_args()
    if not args.path:
        print("Invalid Path Provided")
        exit(1)

    xl_workbook = xlrd.open_workbook(args.path)

    DEFAULT_TEMPLATE_FILE = os.path.join(BASE_DIR, "conf", "dms-task.template")
    JSON_DEFAULT_TEMPLATE_FILE = json.loads(open(DEFAULT_TEMPLATE_FILE).read(), object_pairs_hook=OrderedDict)

    # Load Tasks master sheet from input file
    if 'DMS-Tasks' not in xl_workbook.sheet_names():
        print("DMS-Tasks Sheet not found")
        exit(1)
    tasks_sheet = xl_workbook.sheet_by_name('DMS-Tasks')
    num_cols, num_rows = tasks_sheet.ncols, tasks_sheet.nrows
    tags_sheet = xl_workbook.sheet_by_name('DMS-Tags')

    tags = list()
    for r in range(1, tags_sheet.nrows):
        tags.append(form_tag(str(get_cell_str(tags_sheet.cell(r,1))), str(get_cell_str(tags_sheet.cell(r,2)))))

    for r in range(1,num_rows):
        task_name, task_description = tasks_sheet.cell(r, 1).value, tasks_sheet.cell(r, 2).value

        # Reading ARNs from config if not None will be assigned
        source_arn, target_arn, replica_arn = tasks_sheet.cell(r, 3).value, tasks_sheet.cell(r, 4).value, tasks_sheet.cell(r, 5).value
        cdc_start_time = tasks_sheet.cell(r,6)
        schema_name = tasks_sheet.cell(r,7).value
        # Task specific sheet

        task_sheet = xl_workbook.sheet_by_name(task_name)
        params = {
            "source_arn" : source_arn,
            "target_arn" : target_arn,
            "replica_arn" : replica_arn,
            "migration_type" : args.type
        }
        if len(tags) > 0: params["tags"] = tags
        if args.type != 'full-load' and cdc_start_time.value and cdc_start_time.ctype != xlrd.XL_CELL_TEXT: params["cdc_start_time"] = get_cell_str(cdc_start_time)

        create_task(task_name,
                    schema_name,
                    task_description,
                    task_sheet,
                    **params)
    exit(0)
