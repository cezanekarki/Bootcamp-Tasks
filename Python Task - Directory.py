# Databricks notebook source
# MAGIC %fs ls dbfs:///

# COMMAND ----------

import os 
os.listdir()

# COMMAND ----------

os.getcwd()

# COMMAND ----------

f=open("hello there ","w")
print(f)
f.close()

# COMMAND ----------

with open("hello.txt","w")as f:
    pass

# COMMAND ----------

with open("hello.txt","w")as f:
    f.write("hello world")

with open("hello.txt","r")as f:
    print(f.read())


# COMMAND ----------

with open("hello.txt","w")as f:

    f.write("hello world")
    f.write("hello worldsss")
 
with open("hello.txt","r")as f:
    print(f.read())


# COMMAND ----------

with open("hello.txt","a+")as f:
    f.write("Hello there \n new new new")

with open("hello.txt","r")as f:
    print(f.read())


# COMMAND ----------

with open("hello.txt","r+") as f:
    # f.write("Hello there \n new new new")
    f.seek(2)
    print(f.read())

# COMMAND ----------

import json
d={
    "a":"shishir",
    "b":"paudel",
    "c":23
},{
    "a":"shishir",
    "b":"paudel",
    "c":23
},{
    "a":"shishir",
    "b":"paudel",
    "c":23
}
print(json.dumps(d))

# COMMAND ----------

type(json.dumps(d))


# COMMAND ----------

import json
d={
    "a":"shishir",
    "b":"paudel",
    "date":datetime(1990,1,1)
}
print(json.dumps(d,default=str))
# print(d)

# COMMAND ----------

import json
string = '{"name": "John Doe", "dob": "1990-01-01 00:00:00", "city": "Anytown", "email": "john.doe@example.com","is_student": false}'
print(json.loads(string))

# COMMAND ----------

with open("record.json","w")as f:
    pass

# COMMAND ----------

with open("record.json","r")as f:
    print(f.read())

# COMMAND ----------

import json

string = '{"name": "John Doe", "dob": "1990-01-01 00:00:00", "city": "Anytown", "email": "john.doe@example.com", "is_student": false}'


data = json.loads(string)

with open("record.json", "w") as f:
    json.dump(data, f)

# COMMAND ----------

with open("record.json","r")as f:
    print(json.dumps(data,default=str))

# COMMAND ----------



data["dob"] = datetime.strptime(data["dob"], "%Y-%m-%d %H:%M:%S")

# Write the updated data to a JSON file
with open("record.json", "w") as f:
    json.dump(data, f)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Requests
import requests
data={
    "title":"BMW",
}
response =requests.post("https://dummyjson.com/products/add",json=data)
print(response.text)

# COMMAND ----------

from requests import session
s = session()
# print(s)
datas=s.post("https://dummyjson.com/products/add",data)
print(datas.text)


# COMMAND ----------


