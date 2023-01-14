import database_main
import mycredlib as m
import numpy as np
import pandas as pd

try:
    db_file = 'normalized.db'  # Initializing a variable with db name
    # Creating a connection to db
    conn_norm = m.create_connection(db_file, False)
except Exception as msg:
    print(msg)

try:
    with conn_norm:
        # Deleting the record where the amount applied for was 0 or amount credited is 0 and contract status not equal to "Cancelled".
        cursor = conn_norm.cursor()
        sql = 'delete from PREVIOUS_DATA where (amt_application = 0 or AMT_CREDIT=0) and NAME_CONTRACT_STATUS != "Canceled";'
        cursor.execute(sql)

        #The below code, will give you a dictionary of percent of blanks in each column.
        """cur = conn_norm.cursor()
        cur.execute('PRAGMA table_info(ACTUAL_DATA);')
        table_schema = cur.fetchall()
        select_sql_1 = "SELECT "
        select_sql_2 = ''
        select_sql_3 =" FROM ACTUAL_DATA "

        col_list = []
        for ele in table_schema:
            col_list.append(ele[1])
            if select_sql_2 != '':
                select_sql_2 += ",ROUND(((SUM(CASE WHEN "+ele[1]+" = '' THEN 1 ELSE 0 END)1.0)/count())*100,2) AS "+ele[1]
            else:
                select_sql_2 += "ROUND(((SUM(CASE WHEN "+ele[1]+" = '' THEN 1 ELSE 0 END)1.0)/count())*100,2) AS "+ele[1]
        select_sql = select_sql_1+select_sql_2+select_sql_3
        with conn_norm:
            cursor = conn_norm.cursor()
            cursor.execute(select_sql)
            blank_list = cursor.fetchall()
            percent_blank = dict(zip(col_list,blank_list[0])) """

        # Removing columns with more than 30% null values.
        null_list = ['AMT_ANNUITY', 'AMT_DOWN_PAYMENT', 'AMT_GOODS_PRICE', 'RATE_DOWN_PAYMENT', 'RATE_INTEREST_PRIMARY',
                     'RATE_INTEREST_PRIVILEGED', 'NAME_TYPE_SUITE', 'CNT_PAYMENT', 'DAYS_FIRST_DRAWING', 'DAYS_FIRST_DUE',
                     'DAYS_LAST_DUE_1ST_VERSION', 'DAYS_LAST_DUE', 'DAYS_TERMINATION', 'NFLAG_INSURED_ON_APPROVAL']
        for i in null_list:
            sql = 'ALTER TABLE PREVIOUS_DATA DROP ' + i + ';'
            cursor.execute(sql)

        # Deleting the records where PRODUCT_COMBINATION is null.
        sql = "delete from PREVIOUS_DATA where PRODUCT_COMBINATION = '';"
        cursor.execute(sql)
except Exception as msg:
    print(msg)

# Removing outliers from previous_data table
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
    sql = 'pragma table_info(PREVIOUS_DATA);'
    df_sql = pd.read_sql_query(sql, conn_norm)
    num_col_names = list(df_sql[df_sql['type'] != 'TEXT']['name'])

    outlier_list = []
    try:
        for i in num_col_names[2:]:
            if i in ['NFLAG_LAST_APPL_IN_DAY', 'SK_ID_PREV', 'SK_ID_CURR']:
                continue
            else:
                col_name = i
                sql = 'select SK_ID_PREV, '+i+' from PREVIOUS_DATA;'
                df_sql = pd.read_sql_query(sql, conn_norm)
                df_out = remove_outlier(df_sql, col_name)
                outlier_list += list(df_out['SK_ID_PREV'])
    except Exception as msg:
        print(msg)

    outlier_list = list(set(outlier_list))

    outlier_tuple = tuple(outlier_list)

    # Since they are many categories inside the categorical column, we are combining less frequent categories into a single category called "Others"
    with conn_norm:
        cursor = conn_norm.cursor()

        sql = f'delete from PREVIOUS_DATA where SK_ID_PREV in {outlier_tuple};'
        cursor.execute(sql)

        sql = 'update PREVIOUS_DATA set NAME_CASH_LOAN_PURPOSE = "Others" where NAME_CASH_LOAN_PURPOSE not in ("XAP","XNA","Repairs");'
        cursor.execute(sql)

        sql = 'update PREVIOUS_DATA set CODE_REJECT_REASON = "Others" where CODE_REJECT_REASON not in ("XAP","HC","LIMIT");'
        cursor.execute(sql)

        sql = 'update PREVIOUS_DATA set NAME_GOODS_CATEGORY = "Others" where NAME_GOODS_CATEGORY not in ("XNA","Mobile","Computers");'
        cursor.execute(sql)

        sql = 'update PREVIOUS_DATA set CHANNEL_TYPE = "Others" where CHANNEL_TYPE not in ("Credit and cash offices","Country-wide","Stone");'
        cursor.execute(sql)

        sql = 'update PREVIOUS_DATA set NAME_SELLER_INDUSTRY = "Others" where NAME_SELLER_INDUSTRY not in ("XNA","Connectivity","Consumer electronics");'
        cursor.execute(sql)

        sql = 'update PREVIOUS_DATA set PRODUCT_COMBINATION = "Others" where PRODUCT_COMBINATION not in ("Cash","POS mobile with interest","POS household with interest","Cash X-Sell: middle");'
        cursor.execute(sql)

        # Changing the days to positive value
        sql = 'update PREVIOUS_DATA set DAYS_DECISION = abs(DAYS_DECISION) where DAYS_DECISION < 0;'
        cursor.execute(sql)

        sql = 'ALTER TABLE PREVIOUS_DATA DROP SELLERPLACE_AREA;'
        cursor.execute(sql)

        # Categorizing the numerical values
        sql = 'alter table PREVIOUS_DATA add days_decision_category text;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set days_decision_category = "0-500" where days_decision between 0 and 500;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set days_decision_category = "501-1000" where days_decision between 501 and 1000;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set days_decision_category = "1001-1500" where days_decision between 1001 and 1500;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set days_decision_category = "1501-2000" where days_decision between 1501 and 2000;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set days_decision_category = ">2000" where days_decision>2000;'
        cursor.execute(sql)
        sql = 'alter table PREVIOUS_DATA add HOUR_APPR_PROCESS_START_CATEGORY text;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set HOUR_APPR_PROCESS_START_CATEGORY = "0-4" where HOUR_APPR_PROCESS_START between 0 and 4;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set HOUR_APPR_PROCESS_START_CATEGORY = "5-8" where HOUR_APPR_PROCESS_START between 5 and 8;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set HOUR_APPR_PROCESS_START_CATEGORY = "9-12" where HOUR_APPR_PROCESS_START between 9 and 12;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set HOUR_APPR_PROCESS_START_CATEGORY = "13-16" where HOUR_APPR_PROCESS_START between 13 and 16;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set HOUR_APPR_PROCESS_START_CATEGORY = ">16" where HOUR_APPR_PROCESS_START > 16;'
        cursor.execute(sql)

        sql = 'alter table PREVIOUS_DATA add AMT_APPLICATION_CATEGORY text;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set AMT_APPLICATION_CATEGORY = "0" where AMT_APPLICATION = 0;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set AMT_APPLICATION_CATEGORY = "1-100000" where AMT_APPLICATION between 1 and 100000;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set AMT_APPLICATION_CATEGORY = "100001-200000" where AMT_APPLICATION between 100001 and 200000;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set AMT_APPLICATION_CATEGORY = "200001-300000" where AMT_APPLICATION between 200001 and 300000;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set AMT_APPLICATION_CATEGORY = "300001-400000" where AMT_APPLICATION between 300001 and 400000;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set AMT_APPLICATION_CATEGORY = "400001-500000" where AMT_APPLICATION between 400001 and 500000;'
        cursor.execute(sql)

        sql = 'alter table PREVIOUS_DATA add AMT_CREDIT_CATEGORY text;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set AMT_CREDIT_CATEGORY = "0" where AMT_CREDIT = 0;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set AMT_CREDIT_CATEGORY = "1-100000" where AMT_CREDIT between 1 and 100000;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set AMT_CREDIT_CATEGORY = "100001-200000" where AMT_CREDIT between 100001 and 200000;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set AMT_CREDIT_CATEGORY = "200001-300000" where AMT_CREDIT between 200001 and 300000;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set AMT_CREDIT_CATEGORY = "300001-400000" where AMT_CREDIT between 300001 and 400000;'
        cursor.execute(sql)
        sql = 'update PREVIOUS_DATA set AMT_CREDIT_CATEGORY = ">400000" where AMT_CREDIT >400000;'
        cursor.execute(sql)

    # Selecting the below features to build customer clustering model from previous_data
    sql = 'select SK_ID_CURR, NAME_CONTRACT_TYPE, AMT_CREDIT_CATEGORY, \
    WEEKDAY_APPR_PROCESS_START, HOUR_APPR_PROCESS_START_CATEGORY, FLAG_LAST_APPL_PER_CONTRACT, NFLAG_LAST_APPL_IN_DAY, \
    NAME_CASH_LOAN_PURPOSE, NAME_CONTRACT_STATUS, days_decision_category, NAME_PAYMENT_TYPE, CODE_REJECT_REASON, \
    NAME_CLIENT_TYPE, NAME_GOODS_CATEGORY, NAME_PORTFOLIO, NAME_PRODUCT_TYPE, CHANNEL_TYPE, NAME_SELLER_INDUSTRY, \
    NAME_YIELD_GROUP, PRODUCT_COMBINATION from PREVIOUS_DATA;'
    df_sql = pd.read_sql_query(sql, conn_norm)

    df_sql.drop('NAME_PORTFOLIO', axis=1, inplace=True)

    x = df_sql.drop('SK_ID_CURR', axis=1)

    from sklearn.cluster import KMeans

    x_encoded = pd.get_dummies(x, columns=['NAME_CONTRACT_TYPE', 'AMT_CREDIT_CATEGORY',
                                           'WEEKDAY_APPR_PROCESS_START', 'HOUR_APPR_PROCESS_START_CATEGORY',
                                           'FLAG_LAST_APPL_PER_CONTRACT',
                                           'NAME_CASH_LOAN_PURPOSE', 'NAME_CONTRACT_STATUS',
                                           'days_decision_category', 'NAME_PAYMENT_TYPE', 'CODE_REJECT_REASON',
                                           'NAME_CLIENT_TYPE', 'NAME_GOODS_CATEGORY', 'NAME_PRODUCT_TYPE',
                                           'CHANNEL_TYPE', 'NAME_SELLER_INDUSTRY', 'NAME_YIELD_GROUP',
                                           'PRODUCT_COMBINATION'])

    kmeans = KMeans(3)
    kmeans.fit(x_encoded)
    identified_clusters = kmeans.fit_predict(x_encoded)

    df_sql_with_clusters = df_sql.copy()
    df_sql_with_clusters['CLUSTER'] = identified_clusters

    cluster_dict = {}
    for index, rows in df_sql_with_clusters[['SK_ID_CURR', 'CLUSTER']].iterrows():
        key = rows['SK_ID_CURR']
        value = rows['CLUSTER']
        if key not in cluster_dict.keys():
            cluster_dict.update({key: [value]})
        else:
            cluster_dict[key].append(value)

    import statistics as st
    import random

    for i in cluster_dict.keys():
        mode = st.multimode(cluster_dict[i])
        if len(mode) > 1:
            mode = random.choice(mode)
        else:
            mode = mode[0]
        cluster_dict[i] = mode

    # Creating new column in actual_data_norm with the result of above model
    update_prev_clust_sql = ''
    for key, value in cluster_dict.items():
        update_prev_clust_sql += "UPDATE ACTUAL_DATA_NORM SET PREV_CLUST = " + \
            str(value)+" WHERE SK_ID_CURR ="+str(key)+";"

    m.update_actual_table_cluster(conn_norm, update_prev_clust_sql)
except Exception as msg:
    print(msg)
