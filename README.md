# Cloud Formation Template Generator

This is useful to generate CloudFormation templates for aws resources. 

Currently We support only aws DMS replication tasks by taking csv as input.

### Prerequisites

Python 2.7 and above

```
https://www.python.org/downloads/
```

### Installing


```
git clone ssh://git.amazon.com/pkg/Cloud-formation-generator
```

## Usage

```
usage: cf-generator.py [-h] --path PATH --type
                       {cdc,full-load,full-load-and-cdc}
```
## Running code

```
python cf-generator.py --path=<full path csv> --type=<cdc/full-load/full-load-and-cdc>

```

## Input Parameters definition

| Name | Description | Required|
| :---- |:----------- |:--------|
| SelectionType | include/exclude of a table to a task under selection rule| True |
|TaskName|Name of a task can be duplicated as a task may have multiple include/exclude selection rules| True |
|TaskDescription|Task description | False |
| SchemaName | Schema name to include. this is used in both selection and transformation rules, % is used to include all schemas | True|
| TableName | Tables to include/exclude, % is used to include all tables | True |
| ExcludeColumns | Exclude columns in table. Multiple columns are separated by comma | False |
|FilterColumn | Key column name to add column filter in selection rules, Composite keys are not supported yet | FAlse |
| FilterCondition | Condition to add to column filter (ste/gte/eq/between) | False |
| StartValue | Condition start value or only value when we use gte/ste and eq | False |
| EndValue | Condition End Value only required if we use between | False |
| CdcStartTime | CDC Task start time or SCN number | False |
| taskPrepMode | DO_NOTHING/ DROP_AND_CREATE/ TRUNCATE_BEFORE_LOAD | True |
| maxSubTasks | Number of threads in parallel. Default is 8 | False | 
| lobMode | Enable Lob for a task TRUE/FALSE. Default FALSE | FALSE |
| fullLob | Enable full lob mode TRUE/FALSE. Default False | False |
|chunkSize| Max LOB size or Lob Chunk Size based on fullLob setting | False|
| validation | TRUE/FALSE Default FALSE | FALSE |
| sourceARN | Source end point ARN | TRUE |
| targetARN | Target end point ARN | TRUE |
| repARN | Replicatoin Instance ARN| TRUE |
| ChangeProcessingDdlHandlingPolicy | Controls DDL changes from DMS, Default False| False |
| logging | Enable Logging, Default True| False |
| batchApplyEnabled | Batch Apply, Default False| False || stop_task_with_cache |  Stop a task after a full load completes and cached changes are applied TRUE/FALSE Default FALSE | FALSE |
| stop_task_without_cache | Stop a task before cached changes are applied TRUE/FALSE Default FALSE | FALSE |
| controlSchema | Database schema name for the AWS DMS target Control Tables | False |
| enableHistoryTable | This table provides information about replication history TRUE/FALSE Default FALSE | FALSE |
| enableSuspendTable | This table provides a list of suspended tables TRUE/FALSE Default FALSE | FALSE |
| enableStatusTable | This table provides details about the current task TRUE/FALSE Default FALSE | FALSE |



Note: No Validation on csv file yet.


### Module to run tasks

#### Describe Stacks

To get task ARNs from CloudFormation template to start tasks using cli, output will be saved in csv file.

```
python describeStacks.py --stackName=<stackName>
```

#### Run Replication Tasks

Takes csv file as input which has task arns

```
python startTasks.py <csvpath>
```

## Authors

* **Naveen Koppula**


## License
