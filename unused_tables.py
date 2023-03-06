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
This file takes the logs and calculates when a table was last called, then separates it into 3 types:
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


def unused_table(client, project_name, discount):
    query_meta = """
    WITH calculate_last_call as (
      (
        SELECT * 
        FROM (
                SELECT  *, 
                        ROW_NUMBER() OVER (PARTITION BY table_id,dataset_id,project_id ORDER BY last_run_date desc) AS table_last_call
                FROM `{project_name}.Data_Defender.total_logs`
                            ) 
        WHERE table_last_call = 1
         )
    )
    SELECT
        project_id,
        dataset_id,
        table_id,
        user_email as last_called_by,
        concat(project_id,'.',dataset_id,'.',table_id) as full_table,
        CASE
            WHEN type = 1 THEN 'table'
            WHEN type = 2 THEN 'view'
            WHEN type= 3 THEN 'External' --like google sheets
            ELSE NULL
        END AS type,
        EXTRACT (DATE from TIMESTAMP_MILLIS(creation_time)) AS creation_date,
        COALESCE(last_run_date,'1980-01-11') as last_modified_date,
        CASE
            WHEN DATE(last_run_date) = EXTRACT (DATE from TIMESTAMP_MILLIS(creation_time))
                AND DATE(last_run_date) < DATE_SUB(CURRENT_DATE(), INTERVAL 30 day)  THEN 'never used' --never used
            WHEN DATE(last_run_date) IS NULL 
                AND EXTRACT (DATE from TIMESTAMP_MILLIS(creation_time)) <= DATE_SUB(CURRENT_DATE(), INTERVAL 180 day) then '6 months unused' --older than 6 months
            --WHEN last_run_date < DATE_SUB(CURRENT_DATE(), INTERVAL 180 day)  THEN "6 months unused" --older than 6 months
            WHEN DATE(last_run_date) < DATE_SUB(CURRENT_DATE(), INTERVAL 90 day) AND
                DATE(last_run_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 day) THEN '3 months unused' --bet.3 months and 6 months
            ELSE NULL
        END AS severity_groups,
        ROUND(SUM(size_bytes)/POW(10,9),0) AS size_gb,
        ROUND(((SUM(size_bytes)/POW(10,9))*0.02)) as monthly_cost, 
        ROUND(((SUM(size_bytes)/POW(10,9))*(1-{discount})*0.02))*12 as annual_cost,  
    FROM `{project}.{dataset}.`.__TABLES__ left join calculate_last_call using (project_id,dataset_id,table_id)
    group by 1,2,3,4,5,6,7,8,9
    having severity_groups is not null

    """

    # GENERAL TABLE
    tables_total_df = pd.DataFrame(
        columns=['full_table', 'last_modified_date', 'severity_groups', 'size_gb', 'monthly_cost', 'annual_cost',
                 'last_called_by'])

    projects = [x.project_id for x in client.list_projects()]

    for project in projects:
        datasets = list(client.list_datasets(project))
        if datasets:
            print("Datasets in project {}:".format(project))
            for dataset in datasets:
                try:
                    df = client.query(
                        query_meta.format(project_name=project_name, project=project, dataset=dataset.dataset_id,
                                          discount=discount)).result().to_dataframe()
                    if (len(df) > 0):
                        tables_total_df = pd.concat([tables_total_df, df], ignore_index=True, sort=False)

                except Exception as exe:
                    print('Could not load since: ', str(exe)[:200])
                    pass

    # Changing the type of the data so BQ will be able to load it
    tables_total_df.last_modified_date = tables_total_df.last_modified_date.astype('datetime64[ns]')
    tables_total_df.monthly_cost = tables_total_df.monthly_cost.astype(float)
    tables_total_df.annual_cost = tables_total_df.annual_cost.astype(float)
    tables_total_df.size_gb = tables_total_df.size_gb.astype(float)

    tables_total_df.to_gbq(destination_table='Data_Defender.unused_tables', project_id=project_name,
                           if_exists='replace')  # Loading the dataframe into a BQ table
    print('Finished unused tables')


def main(client, project_name, discount):
    unused_table(client, project_name, discount)
