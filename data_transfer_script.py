import pyodbc
import time
import requests
import json
from datetime import datetime

# Define the SQLite connection details
database = 'AdventureWorks2022'  # Replace with the path or name of your SQLite database file
tables = 'Person.Person'  # Replace with the name of your table in SQLite
server = 'localhost,1433'
username = 'sa'
password = 'yourStrong(!)Password'  # Replace with the same password you used when starting the container
tables_per_cycle = 5


# Define the SQL query to fetch data from the table
query_template = 'SELECT * FROM {}'

# API base url and end point
swagger_base_url = 'https://eguarantorship-api.presta.co.ke/api/v1/'
import_users_endpoint = 'Import/Users'

# get current date and time
current_time = time.time()

# calculate next run time,set to run once every 24 hours
next_run_time = current_time + 24 * 60 *60

# keep track of last fetched data
last_fetched_index = 0

class CustomEncoder(json.JSONEncoder):
    # convert datetime to json acceptable format i.e iso
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def fetch_data():
    global next_run_time,last_fetched_index

    # calculate end index for current cycle
    end_index = last_fetched_index + tables_per_cycle

    # Establish the connection to the SQLite database
    connection = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + 
                                ';UID=' + username + ';PWD=' + password)
    
    cursor =  connection.cursor()

    # list to store fetched data
    fetched_data = []

    

    query = query_template.format(tables)
    cursor.execute(query)

    for row in cursor:
        # Convert the row to a dictionary
        row_dict = dict(zip([column[0] for column in cursor.description], row))
        fetched_data.append(row_dict)
        # fetched_data.append(row)

        current_cycle_tables = fetched_data[last_fetched_index:end_index]
        # print(current_cycle_tables)

    # increment the last fetched index for the next cycle
    last_fetched_index += tables_per_cycle

    # reset last_fetched_index if it exceeds length of table
    if last_fetched_index >= len(fetched_data):
        last_fetched_index = 0

    return current_cycle_tables
    
    cursor.close()
    connection.close()

def transfer_data(data):
    try:
        encoded_data = json.dumps(data, cls=CustomEncoder)
        response = requests.post(f'{swagger_base_url}{import_users_endpoint}',json=encoded_data)
        response.raise_for_status()
        print("Data transfered  successfully")
        return True
    except requests.exceptions.RequestException as e:
        print(f'Error while transferring data to import user API: {str(e)}')
        return False



def is_time_to_run():
    global next_run_time
    return time.time() >= next_run_time

if __name__ == "__main__":
    while True:
        if is_time_to_run():
            data = fetch_data()
            # if data is fetched successfully transfer the data to end point
            if data:
                transfer_data(data)

            next_run_time = time.time() + 24 * 60 * 60
            print(next_run_time)

        time.sleep(86400) # sleep for 24 hours before calling the function again