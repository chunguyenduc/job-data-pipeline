import pandas as pd
import redshift_connector
import numpy as np

df = pd.read_csv('job-261222-0513.csv')
# print(df.head())

df_job = df[['id', 'title', 'company', 'city', 'url', 'created_date']]

df_2 = pd.read_csv('job-261222-0513.csv', sep=',', usecols=['id', 'skills'])
print(df_2.head())
print(type(df_2.loc[0, 'skills']))


with redshift_connector.connect(
    host='redshift-cluster-2.cydvpxzfgfwd.us-east-1.redshift.amazonaws.com',
    database='dev',
    user='duccn',
    password='Chunguyenduc1999'
) as conn:
    with conn.cursor() as cursor:

        # cursor.execute("show table staging.job_info")
        # result: tuple = cursor.fetchall()
        # print(result)

        cursor.write_dataframe(df_job, "staging.job_info")
        # print(cursor)

        # cursor.execute("select * from staging.job_info")
        # result = cursor.fetch_dataframe()
        # print(result)
        pass

        # conn.commit()
