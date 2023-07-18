import pyodbc
import requests
import time
from datetime import datetime
import json
import math

# Define the SQLite connection details
database = 'db_name'  # Replace with the path or name of your SQLite database file
table = 'Wanaanga.dbo.Customer'  # the name of the customer table in SQLite
balances_table = 'Wanaanga.dbo.Balances'  # the name of the balances table in SQLite
server = 'localhost,1433'
username = 'sa'
password = 'yourStrong(!)Password'  # Replace with the same password you used when starting the container
column_id = 'acno'  # the id column of your table in SQLite
telephone = '0726412393'


# Define the Swagger API base URL and endpoint for importing users
swagger_base_url = 'https://eguarantorship-api.presta.co.ke/'  
import_users_endpoint = '/api/v1/members'  

# # Define the columns to fetch from the database
columns_to_fetch = [
    'acno', 'fname', 'mname', 'lname', 'address', 'city', 'idno', 'personalno', 'entrydate',
    'Telephone', 'sex', 'Mandate', 'Employer', 'PhotoPath', 'SignPath', 'officer', 'Photo', 'signature', 'SectorCode',
    'RegionCode', 'Station', 'Occupation', 'YearBirth', 'PhysicalLoc', 'mtype', 'Deleted', 'Earmark', 'TELEXT', 'Remarks',
    'FrontIDPhoto', 'BackIDPhoto', 'Membertype', 'Mobile', 'ROfficerOther', 'ROfficer', 'YearOfBirth', 'stationcode',
    'MobileBankingCust', 'MobileBankingCurrAc', 'Pin_No', 'Closure', 'Email', 'KRA_PIN'
]

# Define the mappings for the columns
column_mappings = {
    'acno': 'memberNumber',
    'fname': 'firstName',
    'lname': 'lastName',
    'idno': 'idNumber',
    'Telephone': 'phoneNumber',
    'Pin_No': 'pinSecret',
    'Email': 'email'
}

# Define the request body for the Swagger API
request_body = {
    "imported": [
        {
            "firstName": "string",
            "pinSecret": "string",
            "lastName": "string",
            "idNumber": "string",
            "memberNumber": "string",
            "phoneNumber": "string",
            "email": "string",
            "totalShares": 0,
            "totalDeposits": 0,
            "committedAmount": 0,
            "availableAmount": 0,
            "memberStatus": "ACTIVE",
            "loanCount": 0,
            "isTermsAccepted": True,
            "fullName": "string",
            "details": {},
        }
    ],
    "failed": [],
    "rowsImported": 1,
    "rowsFailed": 0
}

# Fetch member data from the MSSQL database in batches
def fetch_member_data(start_index, batch_size):
    try:
        connection = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}')
        cursor = connection.cursor()

        query = f"SELECT {', '.join(columns_to_fetch)} FROM {table} WHERE Telephone='{telephone}' ORDER BY {column_id} OFFSET {start_index} ROWS FETCH NEXT {batch_size} ROWS ONLY"
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]

        data = []
        for row in rows:
            record = dict(zip(columns, row))

            # Extract the middle portion from the acno field and remove the first two zeros
            acno = record['acno']
            acno_parts = acno.split('-')
            if len(acno_parts) >= 2:
                memberNumber = acno_parts[1].lstrip('0')  # Remove the first two zeros
                record['acno'] = memberNumber

                # Fetch balances data from the balances table for the same customer
                balances_query = f"SELECT * FROM {balances_table} WHERE acno LIKE 'S01-%{acno_parts[1]}%' OR acno LIKE 'S02-%{acno_parts[1]}%'"

                cursor.execute(balances_query)
                balances_rows = cursor.fetchall()
                balances_columns = [column[0] for column in cursor.description]
                balances_data = [dict(zip(balances_columns, row)) for row in balances_rows]

                # Update the totalShares and totalDeposits fields with balances data
                total_shares = 0
                total_deposits = 0
                for balances_record in balances_data:
                    if balances_record['acno'].startswith('S01'):
                        total_deposits += balances_record['amount']
                    elif balances_record['acno'].startswith('S02'):
                        total_shares += balances_record['amount']

                record['totalShares'] = total_shares
                record['totalDeposits'] = total_deposits

            data.append(record)

        cursor.close()
        connection.close()

        return data

    except Exception as e:
        print(f"Error fetching data from MSSQL: {str(e)}")
        return None

    

class CustomEncoder(json.JSONEncoder):
    # Convert datetime to JSON acceptable format (ISO format)
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)
    
    
# Function to transfer member data to the Import Users API
def transfer_member_data(data):
    try:
        mapped_data = []
        for record in data:
            details = {}
            mapped_record = request_body["imported"][0].copy()
            for column, value in record.items():
                if column in column_mappings:
                    mapped_record[column_mappings[column]] = value
                else:
                    details[column] = {
                        "value": value,
                        "type": "TEXT"
                    }
            mapped_record["details"] = details
            mapped_data.append(mapped_record)

        request_body["imported"] = mapped_data
        encoded_data = json.dumps(request_body, cls=CustomEncoder, indent=2)

        # Send the encoded data to the Import Users API with authorization headers
        headers = {
            "Authorization": "Bearer YOUR_AUTH_TOKEN"
        }
        response = requests.post(f'{swagger_base_url}{import_users_endpoint}', headers=headers, json=encoded_data)
        response.raise_for_status()
        print("Data transferred to the Import Users API successfully.")
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"Error transferring data to the Import Users API: {str(e)}")
        return False

if __name__ == "__main__":
    # Fetch the total number of records
    connection = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}')
    cursor = connection.cursor()
    count_query = f"SELECT COUNT(*) FROM {table} WHERE Telephone='{telephone}'"
    cursor.execute(count_query)
    count = cursor.fetchone()[0]
    cursor.close()
    connection.close()

    batch_size = math.ceil(count / 10)  # Calculate the batch size based on the total number of records
    last_fetched_record = None  # Initialize the last fetched record
    
    while True:
        # Fetch member data from the MSSQL database starting from the last fetched record
        member_data = fetch_member_data(last_fetched_record, batch_size)

        # If data is fetched successfully, transfer it to the Import Users API
        if member_data:
            if transfer_member_data(member_data):
                # Get the last fetched record from the fetched data
                last_fetched_record = member_data[-1][column_id]

                time.sleep(86400)  # Sleep for 24 hours (86400 seconds) before fetching and transferring data again

        # If no data is fetched, sleep for a shorter interval before retrying
        else:
            time.sleep(300)  # Sleep for 5 minutes before retrying
