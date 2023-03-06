# MIT License

# Copyright (c) 2023 HUMAN Security.

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
This file takes the logs and calculates when a column was last called, then separates it into 3 types:
    1. never used - When a table was created and only used the same day (has to be at least 30 days old)
    2. 6 months unused - When a table isn't in the logs then it means it hasn't been called in the past 6 months (Since the retention is 6 months)
    2. X months unused - When a table isn't in the logs than it means it hasn't been called in the time period of your organization's retention of logs
    3. 3 months unused - When a table hasn't been called in the past 3-6 months
Input:
    1. Relevant credentials for BQ access
    2. Table of logs from total_logs.py
Output: BQ table of the last time a column was called and how much money your organization pays for it
"""

import pandas as pd


def unused_column(client, project_name):
    unused_columns_query = """ 

        WITH used_columns as(
        SELECT   
                CONCAT(project_id,".",dataset_id,".",table_id) as table_name,
                project_id,
                dataset_id,
                table_id,
                column_name,
                MAX(extract(date from last_run_date))  as last_run_date
        FROM `{project_name}.Data_Defender.used_columns_open_source` 
        GROUP BY 1,2,3,4,5
    ),

    all_columns as (
        SELECT 
            DISTINCT CONCAT(table_catalog,".",table_schema,".",table_name) as table_name,
            table_catalog AS project_id,
            table_schema AS dataset_id,
            REGEXP_REPLACE(table_name, r'\_\d{numbers}', "") AS table_id,
            column_name
        FROM `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMNS
    ),

    unused_columns_wd as (
    SELECT 
        DISTINCT all_columns.project_id,
        all_columns.dataset_id,
        all_columns.table_id,
        all_columns.table_name,
        all_columns.column_name,
        used_columns.last_run_date,
        CASE 
        WHEN used_columns.column_name IS NULL THEN "more than 6"
        WHEN last_run_date < DATE_SUB(CURRENT_DATE(), interval 90 day) THEN "bet.3 and 6 m"
        ELSE "used in last 3 m"
        END AS severity_group
    FROM all_columns left join used_columns USING (column_name,table_id)
    ),

    unused_columns as (
    SELECT * FROM (
        SELECT DISTINCT   
                          project_id,
                          dataset_id,
                          table_id,
                          table_name,
                          column_name,
                          last_run_date,
                          severity_group, 
                          ROW_NUMBER() OVER (PARTITION BY table_name, column_name ORDER BY last_run_date DESC) as last_
        FROM unused_columns_wd
        WHERE severity_group<>"used in last 3 m"
        ) 
    WHERE last_ = 1
    )

    SELECT DISTINCT
        REGEXP_REPLACE(unused_columns.table_name, r'\_\d{numbers}', "") table_name,
        unused_columns.column_name,
        unused_columns.last_run_date,
        unused_columns.severity_group
    FROM unused_columns
    group by 1,2,3,4"""

    projects = [x.project_id for x in client.list_projects()]
    total_unused_columns = pd.DataFrame()
    for project in projects:
        datasets = list(client.list_datasets(project))
        if datasets:
            for dataset in datasets:
                try:
                    df = client.query(
                        unused_columns_query.format(project_name=project_name, project=project,
                                                    dataset=dataset.dataset_id, numbers='{8}')).result().to_dataframe()
                    if (len(df) > 0):
                        total_unused_columns = pd.concat([total_unused_columns, df], ignore_index=True, sort=False)

                except Exception as exe:
                    print('Could not load since: ', str(exe)[:200])
                    pass

    total_unused_columns.last_run_date = total_unused_columns.last_run_date.astype('datetime64[ns]')
    total_unused_columns.to_gbq(destination_table='Data_Defender.unused_columns', project_id=project_name,
                                if_exists='replace')
    print('Finished unused columns')


def main(client, project_name):
    unused_column(client, project_name)
    
