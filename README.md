# Data-Defender
### A tool to help organizations improve efficiency and minimize costs of BigQuery data

![alt text](https://img.shields.io/badge/Licence-MIT-green)
![alt text](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10-blue)


## Usage
**Note: Since some packages aren't yet ported to the M1 architecture for macOS you may need to use another operating system in order to run Data-Defender**
To install the package:

```
python -m venv .virtualenv
source .virtualenv/bin/activate
pip install -r requirements.txt
```

Then run `main.py` passing in various values as follows:
```
usage: main.py [-h] --project_name PROJECT_NAME --credential_path CREDENTIAL_PATH --query QUERY [QUERY ...] [--discount DISCOUNT]

Analyse BigQuery tables for usage

optional arguments:
  -h, --help            show this help message and exit
  --project_name PROJECT_NAME
                        Name of the BigQuery project to use
  --credential_path CREDENTIAL_PATH
                        Path to the JSON credentials file used to access BigQuery
  --query QUERY [QUERY ...]
                        Which query to run, valid values are 'unused_tables' and 'unused_columns'
  --discount DISCOUNT   A decimal representation of any discount, if applicable, for BigQuery.
```

You can pass in a single or multiple values for the `query` parameter which controls which checks will be performed. 
Valid values are as follows:
- `unused_tables` - checks for tables that haven't been used recently.
- `unused_columns` - checks for columns that haven't been used recently.

We recommend running `unused_tables` separately from `unused_columns` in order to run faster.
We also recommend running `unused_columns` on specific datasets.

For example to run the `unused_tables` checks with a discount of 0.05% you would do something like this:
```
 python main.py --project_name myProject \
                --credential_path /path/to/my/credentials.json \
                --query unused_tables \
                --discount 0.05
```

### Procedure
When the program is run it will issue a number of queries against tables in the relevant BI `INFORMATION_SCHEMA` for your account. It will then generate summary reports in a database named `Data_Defender` in tables described below. The first time it is run these tables will be created and then updated on each subsequent run. The user calling `main.py` will thus need the relevant permissions in BigQuery to issue the corresponding SELECT and DDL commands.

- `total_logs` - All query types will result in this being generated, it contains a summary of when each table was last accessed.

- `unused_tables` - A report for each unused table will be generated and stored in the unused_tables table.
- `unused_columns` - The `used_columns` query will be run first, and the resulting `used_columns` table will be used to identify the unused columns in the `unused_columns` query.

#### total_logs table
`Schema:`\
`user_email` - The e-mail address of the last person who called this table\
`job_type` - Whether it's a QUERY or VIEW\
`last_run_date` - The timestamp when the table was last queried \
`project_id` - The project ID\
`dataset_id` - The dataset ID\
`table_id` - The table ID\
`query` - The query that called the table\
`last_call` - Internal use, ordering based on timestamp to find the actual last time the table was called

#### unused_tables table
`Schema`:\
`full_table` - Concatenation of project_id+dataset_id+table_id\
`last_modified_date` - The last time the table was modified\
`severity_groups` - How long was this table not queried. Possible values:
* Never Been Used
* Not used for more than 6 m
* Not used for 3 to 6 m

`size_gb` - The size of the table\
`monthly_cost` - The monthly cost of storing the table\
`annual_cost` - The annual cost of storing the table\
`last_called_by` - The last person (email address) who called this table\
`project_id` - The project ID\
`dataset_id` - The dataset ID\
`table_id` - The table ID\
`type` - Whether it's a QUERY or VIEW\
`creation_date` - The creation date of the table\

#### used_columns table
`Schema`:\
`project_id` - The project ID\
`dataset_id` - The dataset ID\
`table_id` - The table ID\
`column_name` - The specific column inside the table\
`last_run_date` - The last timestamp this column was specified in a query


#### unused_columns table
`Schema`:\
`table_name` - Concatenation of project_id+dataset_id+table_id\
`column_name` - The specific column inside the table\
`last_run_date` - The last time this column was specified in a query\
`severity_group` - How long was this column not queried specifically. Possible values:
* Never Been Used
* Not used for more than 6 m
* Not used for 3 to 6 m


## Example of output 

<img width="1381" alt="image" src="https://user-images.githubusercontent.com/68190218/217294100-00a56555-8df3-4298-96f1-484bb0c55638.png">

## Contribute

Any type of contribution is warmly welcome and appreciated ❤️
