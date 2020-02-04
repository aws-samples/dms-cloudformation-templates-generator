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

import sqlite3


def execute(path):
    conn = sqlite3.connect(path)
    c = conn.cursor()

    # Create table

    # Task Details Table
    c.execute('''
        create table task(
            id integer primary key autoincrement,
            task_id VARCHAR(256),
            table_name VARCHAR(256),
            schema_name VARCHAR(256),
            table_status VARCHAR(32),
            task_status VARCHAR(32),
            UNIQUE(task_id,table_name,schema_name)
        );
    ''')

    # Table Stats Table
    c.execute('''
        create table table_status(
            id integer primary key autoincrement,
            schema_name VARCHAR(256),
            table_name VARCHAR(256),
            status VARCHAR(32),
            inserted_on datetime default current_timestamp,
            updated_on datetime,
            UNIQUE(schema_name,table_name)
        );
    ''')

    # Object Stats Table
    c.execute('''
        create table object_status(
            id integer primary key autoincrement,
            schema_name VARCHAR(256),
            table_name VARCHAR(256),
            object_name VARCHAR(256),
            object_type VARCHAR(32),
            query TEXT,
            status VARCHAR(32),
            msg TEXT,
            inserted_on datetime default current_timestamp,
            updated_on datetime,
            UNIQUE(schema_name,table_name,object_name)
        );
    ''')

    conn.close()


def inset_record(path, task_id, schema_name, table_name):
    conn = sqlite3.connect(path)
    c = conn.cursor()
    query = "insert into task (task_id,table_name,schema_name,table_status,task_status) values ('%s','%s','%s','new','new');" % (
        task_id, table_name, schema_name)
    c.execute(query)
    conn.commit()
    conn.close()

