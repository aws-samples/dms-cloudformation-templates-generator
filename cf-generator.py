#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import re
import json
import argparse

from settings import BASE_DIR, PARAMETERS
from apps.common import add_outputs, read_csv
from apps.dms import create_task, task_settings


def generate_template(file_path, migration_type, dbPath=None):
    csv_data = read_csv(file_path)

    final_data = task_settings(csv_data)
    template = dict()
    template["Parameters"] = PARAMETERS
    template['Resources'] = dict()
    
    for task in final_data:
        task_info = final_data.get(task)
        task_name = task_info['name']
        if not task_name:
            continue
        params = {
            "source_arn" : task_info.get('sourceARN',None),
            "target_arn" : task_info.get('targetARN',None),
            "replica_arn" : task_info.get('repARN',None),
            "migration_type" : migration_type,
            "target_prep_mode" : task_info.get('taskPrepMode', 'DO_NOTHING'),
            "max_sub_tasks" : task_info.get('maxSubTasks',8),
            "support_lobs" : task_info.get('lobMode',False),
            "full_lob_mode" : task_info.get('fullLob',False),
            "lob_chunk_size" : task_info.get('chunkSize',32),
            "validation":task_info.get('validation',False),
            "ChangeProcessingDdlHandlingPolicy": task_info.get('ChangeProcessingDdlHandlingPolicy', 'FALSE'),
            "logging": task_info["logging"],
            "BatchApplyEnabled": task_info["batchApplyEnabled"],
            "StopTaskCachedChangesApplied": task_info.get('stop_task_with_cache', False),
            "StopTaskCachedChangesNotApplied": task_info.get('stop_task_without_cache', False),
            "enable_history_table": task_info.get('enable_history_table'),
            "enable_suspend_table": task_info.get('enable_suspend_table'),
            "enable_status_table": task_info.get('enable_status_table'),
            "control_schema": task_info.get('control_schema')
        }
        if migration_type != 'full-load' and task_info.get('CdcStartTime'): params["cdc_start_time"] = int(task_info.get('CdcStartTime'))
        
        template_contents = create_task(task_name, task_info.get('description'
                    ), task_info.get('data'), dbPath=dbPath, **params)
        task_name = re.sub(r"[^a-zA-Z0-9]", '', task_name)
        template['Resources'][task_name]=template_contents
    template['Outputs'] = add_outputs(template["Resources"])
    with open(os.path.join(BASE_DIR, 'output',  os.path.basename(file_path).split('.')[0] + '.json'), 'w'
              ) as writefile:
        json.dump(template, writefile)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DMS Task options')
    parser.add_argument('--path', required=True, help='Source Path xls')
    parser.add_argument('--type', required=True, choices=['cdc','full-load','full-load-and-cdc'], help='cdc/full-load/full-load-and-cdc')
    parser.add_argument('--indexCreation', action='store_const', const=True, default=False, help="Create index-creation module database")
    parser.add_argument('--preProcess', action='store_const', const=True, default=False,
                        help="Execute Pre Processing script for index db creation")
    parser.add_argument('--indexDBPath', required=False, help="DB Path for index-creation module")
    args = parser.parse_args()
    if not args.path:
        print("Invalid Path Provided")
        exit(1)

    dbPath = None
    if args.indexCreation and args.type in ['full-load', 'full-load-and-cdc']:
        dbPath = os.path.join(BASE_DIR, 'index-db') if not args.indexDBPath else args.indexDBPath

    if dbPath and not os.path.exists(dbPath):
        os.mkdir(dbPath)

    if dbPath:
        dbPath = os.path.join(dbPath, 'migration.db')
    if args.preProcess:
        from index_pre_processing import execute
        execute(dbPath)

    if os.path.isfile(args.path):
        generate_template(args.path, args.type, dbPath=dbPath)
    else:
        files = (file for file in os.listdir(args.path) if os.path.isfile(os.path.join(args.path, file)))
        for f in files:
            print(os.path.join(args.path,f))
            generate_template(os.path.join(args.path,f), args.type, dbPath=dbPath)
