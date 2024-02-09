import json
from datetime import datetime
string_data='{"name": "John Doe", "dob": "1990-01-01 00:00:00", "city": "Anytown", "email": "john.doe@example.com", "is_student": false}'

json_load=json.loads(string_data)
print(json_load)

with open('python_task/string_data.json', 'w') as file:
    json.dump(json_load, file)

with open('python_task/string_data.json', 'r') as file:
    json_data=json.load(file)

json_data['dob']=datetime.strptime(json_data['dob'], '%Y-%m-%d %H:%M:%S')
print(json_data['dob']) # checking the date time object
        
print(json_data)

