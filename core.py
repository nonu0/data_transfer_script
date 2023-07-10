import pyodbc
import requests
import time
from datetime import datetime
import json

# Define the MSSQL database connection details
database = 'YourDatabaseName'  # Replace with the name of your MSSQL database
table = 'Wanaanga.dbo.Customer'  # Replace with the name of your table
server = 'localhost,1433'  # Replace with your server details
username = 'sa'  # Replace with your username
password = 'YourPassword'  # Replace with your password

# Define the Swagger API base URL and endpoint for importing users
swagger_base_url = 'https://eguarantorship-api.presta.co.ke/'
import_users_endpoint = '/api/v1/members'

# Define the columns to fetch from the database
columns_to_fetch = [
    'acno', 'fname', 'mname', 'lname', 'address', 'city', 'savings', 'idno', 'personalno', 'Saccomno', 'entrydate',
    'Telephone', 'sex', 'Mandate', 'Employer', 'PhotoPath', 'SignPath', 'officer', 'Photo', 'signature', 'SectorCode',
    'RegionCode', 'Station', 'Occupation', 'YearBirth', 'PhysicalLoc', 'mtype', 'Deleted', 'Earmark', 'TELEXT', 'Remarks',
    'FrontIDPhoto', 'BackIDPhoto', 'Membertype', 'Mobile', 'ROfficerOther', 'ROfficer', 'YearOfBirth', 'stationcode',
    'MobileBankingCust', 'MobileBankingCurrAc', 'Pin_No', 'Closure', 'Email', 'KRA_PIN'
]

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
            "details": {
                "additionalProp1": {
                    "value": "string",
                    "type": "BOOLEAN"
                },
                "additionalProp2": {
                    "value": "string",
                    "type": "BOOLEAN"
                },
                "additionalProp3": {
                    "value": "string",
                    "type": "BOOLEAN"
                }
            },
            "isTermsAccepted": True,
            "fullName": "string"
        }
    ],
    "failed": [],
    "rowsImported": 1,
    "rowsFailed": 0
}


# Function to fetch member data from the MSSQL database
def fetch_member_data():
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


# Function to transfer member data to the Import Users API
def transfer_member_data(data):
    try:
        mapped_data = []
        for record in data:
            details = {}
            for key, value in record.items():
                if key not in columns_to_fetch:
                    details[key] = {
                        "value": value,
                        "type": "TEXT"
                    }

            mapped_record = {
                "firstName": record["fname"],
                "pinSecret": "",  # Provide the appropriate value
                "lastName": record["lname"],
                "idNumber": record["idno"],
                "memberNumber": "",  # Provide the appropriate value
                "phoneNumber": record["Telephone"],
                "email": record["Email"],
                "totalShares": 0,
                "totalDeposits": 0,
                "committedAmount": 0,
                "availableAmount": 0,
                "memberStatus": "ACTIVE",
                "loanCount": 0,
                "details": details,
                "isTermsAccepted": True,
                "fullName": f"{record['fname']} {record['lname']}"
            }
            mapped_data.append(mapped_record)

        request_body["imported"] = mapped_data
        encoded_data = json.dumps(request_body, indent=2)

        # Send the encoded data to the Import Users API
        # response = requests.post(f'{swagger_base_url}{import_users_endpoint}', json=encoded_data)
        # response.raise_for_status()
        print("Data transferred to the Import Users API successfully.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error transferring data to the Import Users API: {str(e)}")
        return False


if __name__ == "__main__":
    while True:
        member_data = fetch_member_data()

        if member_data:
            if transfer_member_data(member_data):
                time.sleep(86400)  # Sleep for 24 hours (86400 seconds) before fetching and transferring data again
        else:
            time.sleep(300)  # Sleep for 5 minutes before retrying
