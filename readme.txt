---
# Database to API Data Transfer Script

This script fetches data from a MSSQL database and transfers it to an API endpoint. It is designed to work with a MSSQL database and import the customer data into the eGuarantorship API.

## Prerequisites

- Python 3.x
- Install the required dependencies by running the following command:

    ```
    pip install -r requirements.txt
    ```

## Configuration

1. Update the SQLite connection details in the script:
   - `database`: Replace with the path or name of your SQLite database file.
   - `table`: Replace with the name of your table in SQLite.
   - `server`: Replace with the server details.
   - `username`: Replace with your database username.
   - `password`: Replace with your database password.
   - `column_id`: Replace with the ID column of your table in SQLite.

2. Update the Swagger API details in the script:
   - `swagger_base_url`: Replace with the base URL of the Swagger API.
   - `import_users_endpoint`: Replace with the endpoint for importing users in the API.

3. Define the columns to fetch from the database in the `columns_to_fetch` list.

4. Define the mappings for the columns in the `column_mappings` dictionary. 
    Map the database column names to the corresponding keys in the request body.

## Usage

1. Make sure the required dependencies are installed by running the following command:

    ```
    pip install -r requirements.txt
    ```

2. Run the script:

    ```
    python your_script_name.py
    ```

3. The script will continuously fetch data from the MSSQL database, map the columns to the request body, and transfer the data to the Import Users API endpoint. 
   The script will sleep for 24 hours after each transfer before fetching and transferring data again.
   if no data is fetched the script sleeps for 5 mins(300 secs) before trying to fetch again.

4. Monitor the console output for any error messages or status updates.

---
