import mycredlib as m
import copy
import pandas as pd

db_file = 'normalized.db'  # Initializing a variable with db name
conn_norm = m.create_connection(db_file, True)  # Creating a connection to db
column_ID = []
# Features based on domain knowledge and blank values percentage
cat_col_list = [ 'NAME_CONTRACT_TYPE', 'CODE_GENDER','FLAG_OWN_CAR', 'FLAG_OWN_REALTY','NAME_TYPE_SUITE', 'NAME_INCOME_TYPE','NAME_EDUCATION_TYPE', 'NAME_FAMILY_STATUS', 'NAME_HOUSING_TYPE','WEEKDAY_APPR_PROCESS_START']
num_col_list = ['SK_ID_CURR','TARGET','CNT_CHILDREN','AMT_INCOME_TOTAL','AMT_CREDIT','DAYS_BIRTH', 'DAYS_EMPLOYED','DAYS_ID_PUBLISH','CNT_FAM_MEMBERS', 'HOUR_APPR_PROCESS_START','DAYS_LAST_PHONE_CHANGE','REGION_POPULATION_RELATIVE',  'FLAG_MOBIL', 'FLAG_EMAIL','REGION_RATING_CLIENT_W_CITY']
column_ID = cat_col_list+num_col_list

# Loading the csv files to database as a raw table
path = '/Users/deepakrajmohanraj/Desktop/UB/Project/EAS 503/'
m.load_data_to_db(conn_norm, path + 'application_data.csv', ',', 'actual_data_raw')
m.create_copy_of_table(conn_norm,'actual_data_raw','actual_data',column_ID)
m.load_data_to_db(conn_norm, path + 'previous_application.csv', ',', 'previous_data')
m.load_data_to_db(conn_norm, path + 'columns_description.tsv', '\t', 'column_data')

# Removing blank values from Actual_data
delete_sql = "DELETE FROM ACTUAL_DATA WHERE "
condition_sql = ''
for ele in column_ID:
    if condition_sql != '':
        condition_sql += "OR "+ele+" = '' "
    else:
        condition_sql += ele+" = '' "
delete_sql += condition_sql
with conn_norm:
    cursor = conn_norm.cursor()
    cursor.execute(delete_sql)
    blank_list = cursor.fetchall()
    print(cursor.rowcount)

# Removing outliers from Actual data
def remove_outlier(df_in, col_name):
    q1 = df_in[col_name].quantile(0.25)
    q3 = df_in[col_name].quantile(0.75)
    iqr = q3-q1  # Interquartile range
    fence_low = q1-1.5*iqr
    fence_high = q3+1.5*iqr
    df_out = df_in.loc[~((df_in[col_name] > fence_low) &
                         (df_in[col_name] < fence_high))]
    return df_out

try:
    outlier_list = []
    try:
        for i in num_col_list:
            if i in ['SK_ID_CURR','TARGET','FLAG_MOBIL', 'FLAG_EMAIL','REGION_RATING_CLIENT_W_CITY']:
                continue
            else:
                col_name = i
                sql = 'select SK_ID_CURR, '+i+' from ACTUAL_DATA;'
                df_sql = pd.read_sql_query(sql, conn_norm)
                df_out = remove_outlier(df_sql, col_name)
                outlier_list += list(df_out['SK_ID_CURR'])
    except Exception as msg:
        print(msg)
    outlier_list = list(set(outlier_list))

    outlier_tuple = tuple(outlier_list)
except Exception as msg:
    print(msg)

with conn_norm:
    cursor = conn_norm.cursor()

    sql = f'delete from ACTUAL_DATA where SK_ID_CURR in {outlier_tuple};'
    cursor.execute(sql)
    print(cursor.rowcount)

# Checking for duplicate columns in actual_data and previous_data for categorical tables
# If there exists a duplicate column, create only one categorical table with values appended to it.
actual_data_dist_values = m.get_distinct_column_values(conn_norm, 'actual_data')
date_col_list = ['DAYS_BIRTH',	'DAYS_EMPLOYED',	'DAYS_ID_PUBLISH',	'DAYS_LAST_PHONE_CHANGE']
m.create_new_columns(conn_norm,'ACTUAL_DATA',date_col_list)

# Create categorical columns from actual_data and previoud_data
m.create_categorical_tables(conn_norm, actual_data_dist_values)

# Create the WEEKDAY_APPR_PROCESS_START tables separately to keep the values of the table in the week order
days = ['SUNDAY', 'MONDAY', 'TUESDAY','WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY']
m.load_categorical_tables(conn_norm, 'WEEKDAY_APPR_PROCESS_START', days, False)

# Updating the raw table with the ID from categorical tables
m.update_joins(conn_norm, actual_data_dist_values, 'ACTUAL_DATA')

# Creating new table for actual_data and previous_data with foregin key reference
m.normalize_table(conn_norm, 'ACTUAL_DATA', 'SK_ID_CURR')

