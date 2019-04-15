'''
    File name: generateTaskReports.py
    Author: Mahesh Balumuri (maheshb@amazon.com)
    Date created: 24/09/2018
    Date last modified: 04/10/2018
    Python Version: >=2.7s
'''
import os
import sys
import csv
import json
import re
from datetime import datetime
import subprocess

class AWSCLI:
    def __init__(self):
        pass
    
    def start_replication_task(self,
        ReplicationTaskArn,
        StartReplicationTaskType="start-replication"):
        command = "aws dms start-replication-task"
        if ReplicationTaskArn:
            command =  "{0} --replication-task-arn {1}".format(command,ReplicationTaskArn)
        if StartReplicationTaskType:
            command =  "{0} --start-replication-task-type {1}".format(command,StartReplicationTaskType)
        return self.processCommand(command)

    def processCommand(self,command):
        print (command)
        out,err = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True).communicate()
        #print (out)
        if err:
            print ("Command Error: ",err)
        else:
            output = json.loads(out.strip())
            return output

if __name__=="__main__":
    dmsTaskObject = AWSCLI()
    if os.path.exists(sys.argv[1]):
        with open(sys.argv[1], mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for task in csv_reader:
                dmsTaskObject.start_replication_task(task.get("taskARN"))
