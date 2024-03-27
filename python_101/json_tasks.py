'''
Create a string variable with the below value

{"name": "John Doe", "dob": "1990-01-01 00:00:00", "city": "Anytown", "email": "john.doe@example.com", "is_student": false}

Parse the string with json.loads()

Repeat the same work but this time read the data from the json file record.json that you just created. This time the dob should be datetime object.
'''
import json
from datetime import datetime

# Create a string variable with the below value
json_string = {"name": "John Doe", "dob": "1990-01-01 00:00:00", "city": "Anytown", "email": "john.doe@example.com", "is_student": False}

# Parse the string with json.loads()    
parsed_json = json.loads(json_string)
print(parsed_json)

# Repeat the same work but this time read the data from the json file record.json that you just created. This time the dob should be datetime object.
with open('record.json', 'r') as file:
    data = json.load(file)
    data['dob'] = datetime.strptime(data['dob'], '%Y-%m-%d %H:%M:%S')
    print(data)





