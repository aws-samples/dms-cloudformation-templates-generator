# Create AWS CloudFormation templates for AWS DMS tasks using Microsoft Excel


This is a small command line tool written in Python language that takes an MS Excel workbook having names of tables to be migrated, Amazon Resource Names (ARNs) of DMS Endpoints and DMS Replication Instances to be used as input and generates required DMS tasks’ AWS CloudFormation templates as output. Creation of DMS Endpoints and Replication instances is not addressed by this tool.

### Prerequisites

Python 2.7 and above

```
https://www.python.org/downloads/
```

### Installing XLRD Python module

Tool depends on XLRD Python module, if not already installed proceed to install using PIP as mentioned below.

```
pip install xlrd
```

## Usage

```
python create_task.py --path [PATH_OF_THE_MS_EXCEL_TEMPLATE] --type [cdc | full-load | full-load-and-cdc]

```

## Input arguments

| Name | Description | Required|
| :---- |:----------- |:--------|
|path|Location of the MS Excel template comprising DMS tasks details whose AWS CloudFormation templates needs to be generated.| True |
|type|This argument accepts three different values <br> **cdc** – If the DMS tasks to be created are for Change Data Capture mode only <br>**full-load** – If the DMS tasks to be created are for FULL-LOAD mode only<br>**full-load-and-cdc** – If the DMS tasks to be created are for both FULL-LOAD followed by CDC | True |

## Illustration

Run the below mentioned command with the provided sample arguments to generate sample AWS CloudFormation templates of two DMS tasks i.e. DMS-CHILD and DMS-PARENT that are mentioned in the attached sample MS Excel workbook (dms-tasks.xlsx).

```
python create_task.py --path dms-tasks.xlsx --type cdc

```

The above sample command generates two AWS CloudFormation templates i.e. DMS-CHILD.template and DMS-PARENT.template in the **output** subfolder of this tool's root folder.

## License
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
