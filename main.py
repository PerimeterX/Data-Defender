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


import argparse
import unused_columns
import unused_tables
import total_logs
import used_columns
import os
from google.cloud import bigquery
import sys


def credential_initialize(project_name, credential_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path
    client = bigquery.Client(project=project_name)
    return client, project_name


def main(project_name, credential_path, queries, discount):
    try:
        client, project_name = credential_initialize(project_name, credential_path)
    except Exception as exe:
        print("Could not set up credentials environment:\n")
        print(exe)
        exit(0)

    total_logs.main(client, project_name)
    if "unused_tables" in queries:
        print("Running unused tables check for %s" % project_name)
        unused_tables.main(client, project_name, discount)
    if "unused_columns" in queries:
        print("Running unused columns check for %s" % project_name)
        used_columns.main(client, project_name)
        unused_columns.main(client, project_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyse BigQuery tables for usage")
    parser.add_argument("--project_name", required=True, help="Name of the BigQuery project to use")
    parser.add_argument("--credential_path", required=True,
                        help="Path to the JSON credentials file used to access BigQuery")
    parser.add_argument('--query', required=True, nargs='+',
                        help="Which queries to run, valid values are 'unused_tables' and 'unused_columns'")
    parser.add_argument("--discount", default=0, help="A decimal representation of any discount, if applicable, for BigQuery")
    args = parser.parse_args()

    main(args.project_name, args.credential_path, args.query, args.discount)
