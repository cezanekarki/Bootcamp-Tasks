import json
from datetime import datetime
string_data='{"name": "John Doe", "dob": "1990-01-01 00:00:00", "city": "Anytown", "email": "john.doe@example.com", "is_student": false}'

json_load=json.loads(string_data)
print(json_load)



with open('Python/string_data.json', 'w') as file:
    json.dump(json_load, file)

with open('Python/string_data.json', 'r') as file:
    json_data=json.load(file)
    
if json_data['dob']:
    print("Date of birth is present")
    json_data['dob']=datetime.strptime(json_data['dob'], '%Y-%m-%d %H:%M:%S')
    print(json_data['dob'])
else:
    print("Date of birth is not present")
        
print(json_data)








    

