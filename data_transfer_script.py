import pyodbc
import requests
import time
from datetime import datetime
import json

# Define the SQLite connection details
database = 'db_name'  # Replace with the path or name of your SQLite database file
table = 'Wanaanga.dbo.Customer'  # the name of your table in SQLite
server = 'localhost,1433'
username = 'sa'
password = 'yourStrong(!)Password'  # Replace with the same password you used when starting the container
column_id = 'acno'  # the id column of your table in SQLite


# Define the Swagger API base URL and endpoint for importing users
swagger_base_url = 'https://eguarantorship-api.presta.co.ke/'  
import_users_endpoint = '/api/v1/members'  

# # Define the columns to fetch from the database
columns_to_fetch = [
    'acno', 'fname', 'mname', 'lname', 'address', 'city', 'savings', 'idno', 'personalno', 'Saccomno', 'entrydate',
    'Telephone', 'sex', 'Mandate', 'Employer', 'PhotoPath', 'SignPath', 'officer', 'Photo', 'signature', 'SectorCode',
    'RegionCode', 'Station', 'Occupation', 'YearBirth', 'PhysicalLoc', 'mtype', 'Deleted', 'Earmark', 'TELEXT', 'Remarks',
    'FrontIDPhoto', 'BackIDPhoto', 'Membertype', 'Mobile', 'ROfficerOther', 'ROfficer', 'YearOfBirth', 'stationcode',
    'MobileBankingCust', 'MobileBankingCurrAc', 'Pin_No', 'Closure', 'Email', 'KRA_PIN']

# Define the mappings for the columns
column_mappings = {
    'acno':'memberNumber',
    'fname':'firstName',
    'lname':'lastName',
    'idno':'idNumber',
    'Telephone':'phoneNumber',
    'Pin_No':'pinSecret',
    'Email':'email'
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

# Function to fetch member data from the MSSQL database
def fetch_member_data(data):
    try:
        connection = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}')
        cursor = connection.cursor()

        columns_str = ', '.join(columns_to_fetch)
        query = f'SELECT {columns_str} FROM {table}'

        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]

        data = []
        for row in rows:
            data.append(dict(zip(columns, row)))

        cursor.close()
        connection.close()

        return data
    
    except Exception as e:
        print(f"Error fetching data from MSSQL: {str(e)}")
        return None
    

class CustomEncoder(json.JSONEncoder):
    # convert datetime to json acceptable format i.e iso
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


        # Send the encoded data to the Import Users API
        response = requests.post(f'{swagger_base_url}{import_users_endpoint}', json=encoded_data)
        response.raise_for_status()
        print("Data transferred to the Import Users API successfully.")
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"Error transferring data to the Import Users API: {str(e)}")
        return False

if __name__ == "__main__":
    last_fetched_record = None  # Initialize the last fetched record
    
    while True:
        # Fetch member data from the MSSQL database starting from the last fetched record
        member_data = fetch_member_data(last_fetched_record)

        # If data is fetched successfully, transfer it to the Import Users API
        if member_data:
            if transfer_member_data(member_data):
                # Get the last fetched record from the fetched data
                last_fetched_record = member_data[-1][column_id]

                time.sleep(86400)  # Sleep for 24 hours (86400 seconds)before fetching and transferring data again

        # If no data is fetched, sleep for a shorter interval before retrying
        else:
            time.sleep(300)  # Sleep for 5 minutes before retrying