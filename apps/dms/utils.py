def process_task_data(data_dict):
    """
    Process the data and return dict with AWS DMS Task Name as key.
    """

    final_dict = {}
    for data in data_dict:
        task_name = data['TaskName']
        if task_name in final_dict:
            final_dict[task_name]['data'].append(data)
            pass
        else:
            final_dict[task_name] = {}
            final_dict[task_name]['name'] = task_name
            final_dict[task_name]['taskPrepMode'] = data['taskPrepMode']
            final_dict[task_name]['maxSubTasks'] = data['maxSubTasks']
            final_dict[task_name]['lobMode'] = get_boolean_value(data['lobMode'])
            final_dict[task_name]['fullLob'] = get_boolean_value(data['fullLob'])
            final_dict[task_name]['chunkSize'] = data['chunkSize']
            final_dict[task_name]['sourceARN'] = data['sourceARN']
            final_dict[task_name]['targetARN'] = data['targetARN']
            final_dict[task_name]['validation'] = data['validation']
            final_dict[task_name]['repARN'] = data['repARN']
            final_dict[task_name]['description'] = data['TaskDescription']
            final_dict[task_name]['CdcStartTime'] = data.get('CdcStartTime', None)
            final_dict[task_name]['data'] = []
            final_dict[task_name]['ChangeProcessingDdlHandlingPolicy'] = get_boolean_value(data.get('ChangeProcessingDdlHandlingPolicy'))
            final_dict[task_name]['logging'] = get_boolean_value(data.get('logging', "TRUE"))
            final_dict[task_name]['batchApplyEnabled'] = get_boolean_value(data.get('batchApplyEnabled'))
            final_dict[task_name]['stop_task_with_cache'] = get_boolean_value(data.get('stop_task_with_cache'))
            final_dict[task_name]['stop_task_without_cache'] = get_boolean_value(data.get('stop_task_without_cache'))
            final_dict[task_name]['enable_history_table'] = get_boolean_value(data.get('enableHistoryTable'))
            final_dict[task_name]['enable_suspend_table'] = get_boolean_value(data.get('enableSuspendTable'))
            final_dict[task_name]['enable_status_table'] = get_boolean_value(data.get('enableStatusTable'))
            final_dict[task_name]['control_schema'] = data.get('controlSchema', '')
            final_dict[task_name]['data'].append(data)
    return final_dict


def get_boolean_value(data):
    if data and data.lower() == 'true':
        return True
    return False


def form_condition(col,condition,start_value=None,end_value=None):
    """
    Return Filter Condition for column in AWS DMS Replication Task.
    """

    filter_dict = {'filter-type': 'source', 'column-name': col,
                   'filter-conditions': [{'filter-operator': condition}]}

    if condition == 'between':
        filter_dict['filter-conditions'][0]['start-value'] = str(start_value)
        filter_dict['filter-conditions'][0]['end-value'] = str(end_value)
    else:
        filter_dict['filter-conditions'][0]['value'] = str(start_value)
    return filter_dict