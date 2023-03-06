"""
This file takes the logs and calculated when a column was last called
Input:
    1. Relevant credentials for BQ access
    2. Table of logs from total_logs.py
Output: BQ table of the last time a column was called
"""


import pandas as pd
from google.cloud import bigquery
from sql_metadata import Parser
import re


def columns_parse(query):
    try:
        columns = Parser(query).columns
        return columns
    except Exception as exe:
        print('Could not parse query')
        pass


def used_columns(client, project_name):
    used_columns_df = pd.DataFrame(columns=['dataset_id', 'project_id', 'table_id', 'column_name', 'last_run_date'])
    total_logs_query = """
                        SELECT *
                        FROM `{}.Data_Defender.Total_Logs` 
                        """
    query_logs_df = client.query(total_logs_query.format(project_name)).result().to_dataframe()

    for index, query_log in query_logs_df.iterrows():
        query_log = pd.DataFrame(query_log).reset_index()
        query_log.rename(columns={query_log.columns[1]: 'query_str'}, inplace=True)
        temp_query = str(query_log[query_log['index'] == 'query'].query_str.iloc[0])
        query_columns = columns_parse(temp_query)
        ch = '_'
        # The Regex pattern to match al characters on and after '-'
        pattern = ch + "[0-9]{1,10}.*|\_\*"
        # Remove all characters after the character '-' from string
        query_log.values[5, 1] = re.sub(pattern, '', query_log.values[5, 1])
        if query_columns:
            for col in query_columns:
                temp_df = pd.DataFrame({
                    'last_run_date': str(query_log[query_log['index'] == 'last_run_date'].query_str.iloc[0]),
                    'project_id': str(query_log[query_log['index'] == 'project_id'].query_str.iloc[0]),
                    'dataset_id': str(query_log[query_log['index'] == 'dataset_id'].query_str.iloc[0]),
                    'table_id': str(query_log[query_log['index'] == 'table_id'].query_str.iloc[0]),
                    'column_name': col}, index=[0])
                used_columns_df = pd.concat((used_columns_df, temp_df), ignore_index=True, axis=0)

    used_columns_df = used_columns_df.sort_values(by=['last_run_date'],
                                                      axis = 0, ascending=False)
    used_columns_df = used_columns_df.drop_duplicates(subset=['project_id', 'dataset_id', 'table_id', 'column_name'],
                                                      keep='first')
    used_columns_df.last_run_date = used_columns_df.last_run_date.astype('datetime64[ns]')
    used_columns_df.to_gbq(destination_table='Data_Defender.used_columns', project_id=project_name, if_exists='replace')
    print('Finished used columns')


def main(client, project_name):
    used_columns(client, project_name)
