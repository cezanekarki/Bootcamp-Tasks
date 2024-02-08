# Databricks notebook source
json_string = '{"name": "John Doe", "dob": "1990-01-01 00:00:00", "city": "Anytown", "email": "john.doe@example.com", "is_student": false}'
print(json.loads(json_string))
with open ("record.json", "w") as w:
json.dump(json_string, w, default =str)


# COMMAND ----------

import json
from datetime import datetime
with open("record.json", "r") as file:
    loaded_data = json.load(file)

for entry in loaded_data:
    if 'dob' in entry:
        entry['dob'] = datetime.strptime(entry['dob'], "%Y-%m-%d %H:%M:%S")

print(loaded_data)


# COMMAND ----------

from requests import session
data = {
    "title": "BMW Pencil"
}
response= requests.Post("https://dummyjson.com//products/add", json=data)
print(response.text)
print(response.json())


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


