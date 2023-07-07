# Data Transfer Script

This script fetches data from a Microsoft SQL Server database and transfers it to an API endpoint using Swagger.

## Prerequisites

- Python 3.x
- `pyodbc` library
- `requests` library

## Installation

1. Clone this repository or download the script file.

2. Install the required dependencies by running the following command:

   ```
   pip install pyodbc requests
   ```

## Configuration

Before running the script, make sure to update the following configuration variables in the script:

- `database`: The name or path of your SQLite database file.
- `tables`: The name of the table in the SQLite database from which you want to fetch data.
- `server`: The name or IP address of the SQL Server.
- `username`: The username for accessing the SQL Server.
- `password`: The password for accessing the SQL Server.
- `tables_per_cycle`: The number of tables to fetch in each cycle.
- `query_template`: The SQL query template to fetch data from the table. Replace `{}` with the actual table name.

## Usage

To run the script, execute the following command:

```
python data_transfer_script.py
```

The script will fetch data from the specified SQL Server database and transfer it to the API endpoint. The data transfer process will be repeated in cycles according to the configured time interval.

Note: At the moment, the script is configured to print the data that would be transferred to the API endpoint. Update the `transfer_data()` function to make the actual API request once you have a working endpoint.

## Customization

You can customize the script by modifying the following:

- Update the SQL Server connection details (`server`, `database`, `username`, `password`) with your own database information.
- Replace the `tables` variable with the actual table name from which you want to fetch data.
- Modify the `query_template` variable to match the SQL query required to fetch data from your table.
- Adjust the `tables_per_cycle` variable to control the number of tables to fetch in each cycle.
- Update the `swagger_base_url` and `import_users_endpoint` variables with the actual base URL and endpoint of your Swagger API.

## Important Note

Currently, the script is printing the data that would be transferred to the API endpoint. You need to update the `transfer_data()` function to make the actual API request once you have a working endpoint.

Make sure to handle any errors and exceptions that may occur during the data transfer process.