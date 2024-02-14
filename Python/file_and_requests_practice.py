import json
from datetime import datetime
from requests import Session

#Working with file
def file_read_write():
    person = '{ "name": "John Doe", "dob": "1990-01-01 00:00:00", "city": "Anytown", "email": "john.doe@example.com","is_student": false}'

    my_dict = json.loads(person)

    print(my_dict)

    with open('Python/record.json', 'w') as f:
        json.dump(my_dict, f)

    with open('Python/record.json', 'r') as f:
        data = json.load(f)

    if 'dob' in data:
        data['dob'] = datetime.strptime(data['dob'], '%Y-%m-%d %H:%M:%S')
            
    print(data)

#Working with Session
def post_data():
    s = Session()

    data = {
        "title": "Pencil"
    }

    response = s.post("https://dummyjson.com/products/add", json=data)
    print(response.text)
    print(response.json())

file_read_write()
post_data()
