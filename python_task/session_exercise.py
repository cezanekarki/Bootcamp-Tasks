#Make simple post request to a dummy json with the session

import requests

url = 'https://dummyjson.com/products/add'
data = {'title': 'vBMW Pencil'}

session = requests.Session()

session.headers.update({'Content-Type': 'application/json'})

response = session.post(url, json=data)

print(response.json())