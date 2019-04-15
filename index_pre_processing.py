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
        create table table_stats(
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

