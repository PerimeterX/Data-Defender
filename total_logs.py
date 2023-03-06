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
This file extracts the logs from each project you have permissions to under your BQ account and pushes it into a BQ table
Input: Relevant credentials for BQ access
Output: BQ table with last time a table was called
"""


import pandas as pd
from google.cloud import bigquery
import os

def extracting_logs(client, project_name):
    total_logs_query = """
            SELECT * 
            FROM (
                SELECT  user_email,  
                        job_type, 
                        last_run_date,
                        project_id,
                        dataset_id,
                        table_id,
                        query,
                        row_number() OVER (PARTITION BY project_id,dataset_id,table_id order by last_run_date desc) as last_call
                FROM  (
                    SELECT  user_email,
                            job_type, 
                            date(start_time) as last_run_date,
                            referenced_tables,
                            query,
                    FROM `{}.`.`region-us`.INFORMATION_SCHEMA.JOBS
                    WHERE query IS NOT NULL
                        ),unnest(referenced_tables)
                    )
          where last_call = 1
    """

    projects = [x.project_id for x in client.list_projects()]
    all_logs_df = pd.DataFrame()
    for project in projects:
        # credentials
        client = bigquery.Client(project=project)
        try:
            df = client.query(total_logs_query.format(project)).result().to_dataframe()
            if (len(df) > 0):
                all_logs_df = pd.concat([all_logs_df, df], ignore_index=True, sort=False)

        except Exception as exe:
            print('Could not load project: ', project)
            pass

    all_logs_df.last_run_date = all_logs_df.last_run_date.astype(
        'datetime64[ns]')  # Changing the type of the date so BQ will be able to load it
    all_logs_df.to_gbq(destination_table='Data_Defender.total_logs', project_id=project_name, if_exists='replace')
    print('Finished total logs')


def main(client, project_name):
    extracting_logs(client, project_name)
    
