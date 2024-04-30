import requests
import json
import sqlite3



# Define your headers
headers = {
    'subscription-key': '2384af7c667342ceb5a736fe29f1dc6b'
    # Add other headers as needed
}

endpoints = ["https://data-api.health.gov.au/pbs/api/v3/schedules",]

# Make a request to the original API
response = requests.get('https://data-api.health.gov.au/pbs/api/v3/schedules',headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:

    # Parse the JSON response
    json_data = response.json()

    # Extract data from the 'data' field
    data = json_data.get('data', [])

    # Connect to SQLite database
    conn = sqlite3.connect('pbs_datamart_v3.db')
    cursor = conn.cursor()
    
    # Create a table to store the data
    cursor.execute('''CREATE TABLE IF NOT EXISTS SCHEDULE_T
                  (schedule_code INTEGER PRIMARY KEY, revision_number INTEGER,
                   start_tsp TEXT, effective_date TEXT,
                   effective_month TEXT, effective_year INTEGER,
                   publication_status TEXT)''')
    
    # Insert data into the table
    for item in data:
        revision_number = item["revision_number"]
        start_tsp = item["start_tsp"]
        effective_date = item["effective_date"]
        effective_month = item["effective_month"]
        effective_year = item["effective_year"]
        publication_status = item["publication_status"]
        schedule_code = item["schedule_code"]

        cursor.execute('''INSERT INTO SCHEDULE_T (schedule_code, revision_number, start_tsp, effective_date,
                                                effective_month, effective_year, publication_status)
                        VALUES (?, ?, ?, ?, ?, ?, ?)''',
                        (schedule_code, revision_number, start_tsp, effective_date, effective_month,
                        effective_year, publication_status))

    # Commit changes and close connection
    conn.commit()
    conn.close()

else:
    print('Failed to retrieve data from the original API')
