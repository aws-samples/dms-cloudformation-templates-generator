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

import re
import copy
import json

from settings import DMS_TASK_PARAMS, TABLE_MAPPINGS

from apps.dms.utils import form_condition, get_boolean_value


def create(name, description, task_sheet, migration_type='full-load', source_arn=None,
           target_arn=None, replica_arn=None, tags=None, dbPath=None,
           **extra_args):
    i = 1  # counter for rule-id
    task_name = re.sub(r"[^a-zA-Z0-9]", '', name)
    task_identifier = re.sub(r"[^a-zA-Z0-9\-]", '', name)
    properties = copy.deepcopy(DMS_TASK_PARAMS)

    # Add endpoints and replication instance as perametes

    op_template = properties
    op_template['Properties']['ReplicationTaskIdentifier'] = task_identifier
    op_template['Properties']['MigrationType'] = migration_type
    op_template['Description'] = description

    if source_arn:  # Replace Source ARN in Parameters in task dms cloudformation
        op_template["Properties"]["SourceEndpointArn"] = source_arn
    if target_arn:  # Replace Target ARN in Parameters in task dms cloudformation
        op_template["Properties"]["TargetEndpointArn"] = target_arn
    if replica_arn: # Replace ReplicaARN in Parameters in task dms cloudformation
        op_template["Properties"]["ReplicationInstanceArn"] = replica_arn
    
    rep_task_settings = dict()
    if extra_args.get("logging"):
        rep_task_settings["Logging"] = {
            "EnableLogging" : True
        }
    rep_task_settings["FullLoadSettings"] = {
        "TargetTablePrepMode": "DO_NOTHING",
        "MaxFullLoadSubTasks": 8,
        "CommitRate":10000,
        "StopTaskCachedChangesApplied": False,
        "StopTaskCachedChangesNotApplied": False
    }

    rep_task_settings["ControlTablesSettings"] = {
        "ControlSchema": extra_args["control_schema"] if extra_args.get("control_schema") else "",
        "HistoryTableEnabled": extra_args["enable_history_table"],
        "SuspendedTablesTableEnabled": extra_args["enable_suspend_table"],
        "StatusTableEnabled": extra_args["enable_status_table"]
    }

    if extra_args.get("target_prep_mode"): rep_task_settings["FullLoadSettings"]["TargetTablePrepMode"] = extra_args.get("target_prep_mode")
    if extra_args.get("max_sub_tasks"): rep_task_settings["FullLoadSettings"]["MaxFullLoadSubTasks"] = int(extra_args.get("max_sub_tasks"))

    rep_task_settings["FullLoadSettings"]["StopTaskCachedChangesApplied"] = extra_args["StopTaskCachedChangesApplied"]
    rep_task_settings["FullLoadSettings"]["StopTaskCachedChangesNotApplied"] = extra_args["StopTaskCachedChangesApplied"]
    
    rep_task_settings["TargetMetadata"] = {
        "SupportLobs": False
    }

    # Lob setting if enabled
    if extra_args.get("support_lobs"):
        rep_task_settings["TargetMetadata"]["SupportLobs"] = True
        rep_task_settings["TargetMetadata"]["FullLobMode"] = extra_args.get("full_lob_mode")
        if extra_args.get("full_lob_mode"):
            rep_task_settings["TargetMetadata"]["LobChunkSize"] = int(extra_args.get("lob_chunk_size"))
        else:
            rep_task_settings["TargetMetadata"]["LobMaxSize"] = int(extra_args.get("lob_chunk_size"))
        rep_task_settings["FullLoadSettings"]["CommitRate"] = 50
        rep_task_settings["StreamBufferSettings"] = {"StreamBufferSizeInMB":306}
    
    # Validation settings if validation enabled
    if extra_args.get("validation"):
        rep_task_settings["ValidationSettings"] = {}
        rep_task_settings["ValidationSettings"]["EnableValidation"] = True
        rep_task_settings["ValidationSettings"]["ThreadCount"] = 5
    
    # TaskRecoveryTableEnabled if migration_type is cdc or full-load-cdc
    #if migration_type != 'full-load':
    #     rep_task_settings["TargetMetadata"]["TaskRecoveryTableEnabled"] = True

    if extra_args.get('BatchApplyEnabled',False):
        rep_task_settings["TargetMetadata"]["BatchApplyEnabled"] = True
    if migration_type != 'full-load' and extra_args.get("ChangeProcessingDdlHandlingPolicy", 'FALSE') and extra_args.get("ChangeProcessingDdlHandlingPolicy", 'FALSE').lower() == 'false':
        rep_task_settings["ChangeProcessingDdlHandlingPolicy"] = {
            "HandleSourceTableDropped": False,
            "HandleSourceTableTruncated": False,
            "HandleSourceTableAltered": False
        }
    
    #rep_task_settings["ErrorBehavior"] = {
    #    "ApplyErrorEscalationCount" : 10000000
    #}

    op_template["Properties"]["ReplicationTaskSettings"] = json.dumps(rep_task_settings)

    rules = {'rules': []}
    TABLE_MAPPINGS_TEMP = copy.deepcopy(TABLE_MAPPINGS)
    selection_json = TABLE_MAPPINGS_TEMP['selection']
    transformation_default_json = TABLE_MAPPINGS_TEMP['transformation_default']
    transformation_propagation_default_json = TABLE_MAPPINGS_TEMP['transformation_propagation_default']
    schemas = set()
    rename_trans_rules = list()
    propogation_rules = list()
    for row in task_sheet:
        schema_name = row.get('schemaName')
        schemas.add(schema_name)
        table_name = row.get('tableName')

        exclude_columns_list = row.get('excludeColumns','').split(',')
        exclude_columns = []
        if row.get('excludeColumns'):
            exclude_columns = map(str.strip, exclude_columns_list)
        table_json_op = copy.deepcopy(selection_json)

        table_json_op['rule-id'] = i
        table_json_op['rule-name'] = i
        table_json_op['rule-action'] = row.get("selectionType","include")
        table_json_op['object-locator']['schema-name'] = schema_name
        table_json_op['object-locator']['table-name'] = table_name
        filter_column = row.get('filterColumn')
        if filter_column:
            table_json_op['filters'] = form_condition(filter_column,row.get('filterCondition'),
                                            row.get('startValue'),
                                            row.get('endValue'))

        rules['rules'].append(table_json_op)

        if dbPath and row.get("selectionType", "include") == 'include':
            from index_pre_processing import inset_record
            inset_record(dbPath, task_identifier, schema_name, table_name)

        i += 1

        for exclude_column in exclude_columns:  # Adding Exclude Colllumns Rule to task if exists
            if not exclude_column:
                continue
            exc_json_op = copy.deepcopy(transformation_default_json)
            exc_json_op['rule-type'] = 'transformation'
            exc_json_op['rule-id'] = i
            exc_json_op['rule-name'] = i
            exc_json_op['rule-target'] = 'column'
            exc_json_op['object-locator']['schema-name'] = schema_name
            exc_json_op['object-locator']['table-name'] = table_name
            exc_json_op['object-locator']['column-name'] = exclude_column
            exc_json_op['rule-action'] = 'remove-column'
            rules['rules'].append(exc_json_op)
            i += 1
        
        if row.get('renameTable', None):
            exc_json_op = copy.deepcopy(transformation_default_json)
            exc_json_op['rule-type'] = 'transformation'
            exc_json_op['rule-target'] = 'table'
            exc_json_op['object-locator']['schema-name'] = schema_name
            exc_json_op['object-locator']['table-name'] = table_name
            exc_json_op['rule-action'] = 'rename'
            exc_json_op['value'] = row['renameTable']
            rename_trans_rules.append(exc_json_op)

        if get_boolean_value(row.get('propagation')):
            exc_json_op = copy.deepcopy(transformation_propagation_default_json)
            exc_json_op['rule-type'] = 'table-settings'
            exc_json_op['object-locator']['schema-name'] = schema_name
            exc_json_op['object-locator']['table-name'] = table_name
            exc_json_op['parallel-load'] = {
                "type": "partitions-auto"
            }
            propogation_rules.append(exc_json_op)

    for sc in schemas:
        for trans_rule in TABLE_MAPPINGS_TEMP['transformation']:  # Adding transformation rules to task
            trans_rule_temp = copy.deepcopy(trans_rule)
            trans_rule_temp['rule-id'] = i
            trans_rule_temp['rule-name'] = i
            trans_rule_temp['object-locator']['schema-name'] = sc
            rules['rules'].append(trans_rule_temp)
            i += 1
    if len(rename_trans_rules) > 0:
        for r in rename_trans_rules:
            r['rule-id'] = i
            r['rule-name'] = i
            i += 1
            rules["rules"].append(r)
    if len(propogation_rules) > 0:
        for r in propogation_rules:
            r['rule-id'] = i
            r['rule-name'] = i
            i += 1
            rules["rules"].append(r)
    
    op_template['Properties']['TableMappings'] = json.dumps(rules).replace('"', '"')

    # Add Common tags if exist

    if tags and len(tags) > 0:
        op_template['Properties']['Tags'] = tags

    # Add CDCStartTime if applicable

    if extra_args['cdcStartPosition']:
        op_template['Properties']['CdcStartPosition'] = extra_args['cdcStartPosition']
    elif extra_args['cdcStartTime']:
        op_template['Properties']['CdcStartTime'] = extra_args['cdcStartTime']

    if extra_args['cdcStopPosition']:
        op_template['Properties']['CdcStopPosition'] = extra_args['cdcStopPosition']

    return op_template
