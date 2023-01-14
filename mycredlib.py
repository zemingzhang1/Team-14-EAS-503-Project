import sqlite3
import os
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    # Function to create connectiont to db and deleting the db if it exists
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)
    return conn

def determine_data_type(value):
    # To determine the data type of each cell value
    try:
        data_type = int(value)
        return type(data_type)
    except:
        try:
            data_type = float(value)
            return type(data_type)
        except:
            return type(value)

def determine_data_type_of_list(values):
    # To determine the data type of column based on the individual cell value
    data_type = []
    for ele in values:
        data_type.append(determine_data_type(ele))
    if type('A') in data_type:
        return type('A')
    elif type(3.1) in data_type:
        return type(3.1)
    else:
        return type(3)

def create_dict_from_line(header, line, delimiter):
    # To create dictionary of values with key as the column header and value as the cell value
    new_data = dict(zip(header, line.split(delimiter)))
    return new_data

def read_csv_file(filename, delimiter):
    # To read the CSV file based on the file name and delimiter, as actual_data and previous_data has comma ad delimiter
    # and column_description has tab space as delimiter. This function creates a list of dictionaries of each rows.
    final_list = []
    header_list = []
    with open(filename, 'r') as file:
        for line in file:
            if not line.strip():
                continue
            elif not header_list:
                header_list = line.strip().split(delimiter)
                if 'columns_description' in filename:
                    # In the column_description csv, the first column is just the numbering of rows, hence giving it a column name
                    header_list[0] = 'rowID'
                    # In the column_description csv, the file name column is named as 'Table', we cannot have this as a column name in table, so changing it to 'Table_Name'
                    header_list[header_list.index('Table')] = 'Table_Name'
            else:
                # Two column values has comma in it, hence to handle it while splitting, we have replace its value
                if "Spouse, partner" in line or "Stone, brick" in line:
                    line = line.replace('"Spouse, partner"', "Spouse/partner")
                    line = line.replace('"Stone, brick"', "Stone/brick")
                final_list.append(create_dict_from_line(
                    header_list, line.strip(), delimiter))
    return final_list

def extract_all_fields(data):
    # Extracting all column values as a single dictionary to determine its data type
    data_dic = {}
    for ele in data:
        for key, value in ele.items():
            if value.strip() != '':
                if key not in data_dic.keys():
                    data_dic.update({key: [value]})
                else:
                    data_dic[key].append(value)
    return data_dic

def determine_data_type_of_info_fields(data):
    # To determine the data type of each column
    for key, value in data.items():
        data[key] = determine_data_type_of_list(value)
    return data

def format_data(data, info_field_data_type):
    # Formatting column values based on the datatype found
    for ele in data:
        for key, value in ele.items():
            if value.strip() != '':
                ele[key] = info_field_data_type[key](value)
    return data

def create_table(conn, create_table_sql):
    # Function to create a table in database
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def execute_sql_statement(sql_statement, conn):
    # To execute sql statements
    cur = conn.cursor()
    cur.execute(sql_statement)
    rows = cur.fetchall()
    return rows

def create_raw_tables(conn_norm, format, raw_data, table_name):
    # This function creates a tables with raw data type formatted data from csv and inserts values into it
    create__rawTable_sql = "CREATE TABLE "+table_name+" ("
    create_column_names = ''
    insert_sql = "INSERT INTO "+table_name+"("
    column_names = ''
    values = ''

    if table_name != 'column_data':
        format = dict(sorted(format.items()))

    for key, value in format.items():
        if type('A') == value:
            data_type = 'TEXT'
        elif type(3.1) == value:
            data_type = 'REAL'
        else:
            data_type = 'INTEGER'

        if not create_column_names:
            create_column_names += key + ' ' + data_type + '\n' + ' '
        else:
            create_column_names += ',' + key + ' ' + data_type + '\n' + ' '
        if not column_names:
            column_names += key
            values += '?'
        else:
            column_names += ',' + key
            values += ',?'
    else:
        create__rawTable_sql += create_column_names + ");"
        insert_sql += column_names + ") VALUES(" + values + ")"

    insert_list = []

    for ele in raw_data:
        row = []
        if table_name != 'column_data':
            ele = dict(sorted(ele.items()))
        for key, value in ele.items():
            row.append(value)
        if table_name == 'column_data':
            if len(row) < len(format.keys()):
                for i in range(len(format.keys()) - len(row)):
                    row.append('')
        insert_list.append(tuple(row))

    with conn_norm:
        try:
            create_table(conn_norm, create__rawTable_sql)
            conn_norm.executemany(insert_sql, insert_list)
        except Exception as msg:
            print(msg)

def load_data_to_db(conn_norm, filename, delimiter, tablename):
    # Reading data from csv files and loading them into database
    data = read_csv_file(filename, delimiter)
    extract = extract_all_fields(data)
    format = determine_data_type_of_info_fields(extract)
    raw_data = format_data(data, format)
    create_raw_tables(conn_norm, format, raw_data, tablename)

def get_distinct_column_values(conn_norm, tablename):
    # Getting ditinct values from categorical columns to create stand alone tables for foreign key reference
    with conn_norm:
        try:
            cur = conn_norm.cursor()
            # This query will bring up the column info from a table
            cur.execute('PRAGMA table_info('+tablename+');')
            table_schema = cur.fetchall()
            data_dic = {}
            for ele in table_schema:
                values_list = []
                if ele[2] == 'TEXT':
                    # Updating values as 'NA' to empty cell values under catergorical columns
                    cur.execute("UPDATE " + tablename + " SET " +
                                ele[1]+"='Not Available' WHERE "+ele[1]+"=''")
                    # Selecting distinct values from a column
                    cur.execute('SELECT DISTINCT ' +
                                ele[1] + ' FROM ' + tablename)
                    distinct_values = cur.fetchall()
                    for e in distinct_values:
                        values_list.append(e[0])
                    data_dic.update({ele[1]: values_list})
        except Exception as msg:
            print(msg)
    return data_dic

def update_joins(conn_norm, data, table_name):
    # Updating the raw table with the ID from categorical tables
    inner_join_data_sql = ''
    for ele in data.keys():
        inner_join_data_sql += ("UPDATE "+table_name+" SET "+ele+" = (select id from "+ele+" where " +
                                ele+" = "+table_name+"."+ele+");")  # Updating the ID from categorical tables to raw table
    with conn_norm:
        try:
            crsr = conn_norm.cursor()
            crsr.executescript(inner_join_data_sql)
        except Exception as msg:
            print(msg)

def create_categorical_tables(conn_norm, actual_data_dist_values):
    # Creating separate tables for categorical columns
    for key, value in actual_data_dist_values.items():
        if key != 'WEEKDAY_APPR_PROCESS_START':  # Excluding days columns, as it has been handled separately to create the values in days order rather than alphabeutical order
            load_categorical_tables(conn_norm, key, value, True)

def load_categorical_tables(conn_norm, table_name, data, sort=False):
    # Creating categorical tables and inserting values into them
    create__rawTable_sql = "CREATE TABLE "+table_name + \
        " (ID INTEGER NOT NULL PRIMARY KEY, \n" + table_name + ' TEXT NOT NULL);'
    insert_sql = "INSERT INTO "+table_name+"(" + table_name + ") VALUES(?)"
    insert_list = []

    if sort:
        data.sort()

    for ele in data:
        row = []
        if ele.strip():
            row.append(ele)
            insert_list.append(tuple(row))

    with conn_norm:
        try:
            create_table(conn_norm, create__rawTable_sql)
            conn_norm.executemany(insert_sql, insert_list)
        except Exception as msg:
            print(msg)

def normalize_table(conn_norm, table_name, PK_COLUMN):
    # Normalizing the raw table by creating foreign key reference from categorical columns
    with conn_norm:
        try:
            foreign_key_on_sql = 'PRAGMA foreign_keys = ON;'
            create__rawTable_sql = "CREATE TABLE "+table_name+"_NORM ( "
            foreign_key_ref_sql = ''
            cur = conn_norm.cursor()
            cur.execute(foreign_key_on_sql)
            # This query will bring up the column info from a table
            cur.execute('PRAGMA table_info('+table_name+');')
            table_schema = cur.fetchall()
            data_dic = {}

            for ele in table_schema:
                if ele[1] == PK_COLUMN:
                    create__rawTable_sql += ele[1] + ' ' + \
                        ele[2] + ' NOT NULL PRIMARY KEY,'
                elif ele[2] == 'TEXT':
                    create__rawTable_sql += ele[1] + ' INTEGER NOT NULL,'
                    foreign_key_ref_sql += 'FOREIGN KEY(' + \
                        ele[1]+') REFERENCES '+ele[1]+'(ID),'
                else:
                    create__rawTable_sql += ele[1] + \
                        ' ' + ele[2] + ' NOT NULL,'
            else:
                # Removing the last semicolon from the last element in the create table script
                foreign_key_ref_sql = foreign_key_ref_sql[:len(
                    foreign_key_ref_sql)-1]
                create__rawTable_sql += foreign_key_ref_sql+')'

                cur.execute("DROP TABLE IF EXISTS "+table_name +
                            "_NORM")  # Creating normalized tables
                create_table(conn_norm, create__rawTable_sql)
                cur.execute("SELECT * FROM "+table_name)
                insert_list = cur.fetchall()
                insert_sql = "INSERT INTO "+table_name + \
                    "_NORM VALUES(?"+",?"*(len(table_schema)-1)+")"
                conn_norm.executemany(insert_sql, insert_list)
        except Exception as msg:
            print(msg)
    return data_dic

def update_actual_table_cluster(conn_norm, update_prev_clust_sql):
    # Creating a new column in actual_data_norm with the model output from previous_data
    with conn_norm:
        try:
            crsr = conn_norm.cursor()
            create_new_col_sql = "ALTER TABLE ACTUAL_DATA_NORM ADD PREV_CLUST INTEGER DEFAULT 3 NOT NULL"
            crsr.execute(create_new_col_sql)
            crsr.executescript(update_prev_clust_sql)
        except Exception as msg:
            print(msg)

def create_copy_of_table(conn_norm,from_table,to_table,col_list):
    # Creating a copy of actual_data_raw table into actual_data(stagging table)
    select_col_list = ''
    col_list.sort()
    for ele in col_list:
        if not select_col_list:
            select_col_list += ele
        else:
            select_col_list += ","+ele
    with conn_norm:
        try:
            crsr = conn_norm.cursor()
            copy_table_sql = "CREATE TABLE "+to_table+" AS SELECT "+select_col_list+" FROM "+from_table
            crsr.execute(copy_table_sql)
        except Exception as msg:
            print(msg)

def create_new_columns(conn_norm, table_name, col_list):
    # Creating new column for date columns for converting negative data into years
        try:
            crsr = conn_norm.cursor()
            create_new_col_sql = ''
            update_prev_clust_sql = ''
            for ele in col_list:
                create_new_col_sql += "ALTER TABLE "+table_name+" ADD "+ele+"_y INTEGER DEFAULT 0 NOT NULL;"
                update_prev_clust_sql += "UPDATE "+table_name+" SET "+ele+"_y = CASE WHEN "+ele+" <0 THEN floor(("+ele+"*-1)/365) ELSE "+ele+" END ;"
            with conn_norm:
                crsr.executescript(create_new_col_sql)
                crsr.executescript(update_prev_clust_sql)
        except Exception as msg:
            print(msg)