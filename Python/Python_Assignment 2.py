# Databricks notebook source
data_dictionary = {
    "company": {
        "name": "TechGiant",
        "industry": "Technology",
        "locations": {
            "headquarters": "New York",
            "branches": ["San Francisco", "Berlin", "Tokyo"]
        },
        "departments": {
            "development": {
                "employees": 120,
                "projects": ["App Development", "Software Maintenance"]
            },
            "marketing": {
                "employees": 45,
                "projects": ["Brand Campaign", "Social Media"]
            },
            "sales": {
                "employees": 70,
                "projects": ["Domestic Sales", "International Sales"]
            }
        }
    },
    "market_info": {
        "competitors": ["DataCorp", "InnovaCon", "NextGenTech"],
        "market_trends": {
            "current": "AI and Machine Learning",
            "emerging": "Quantum Computing"
        }
    }
}

print(data_dictionary)

# COMMAND ----------

print("Name of the company->", data_dictionary['company']['name'])

print("No of Employees in Development Dept.-> ", data_dictionary['company']['departments']['development']['employees'])

print("Emerging Trends-> ", data_dictionary['market_info']['market_trends']['emerging'])

print("Branch Locations-> ", data_dictionary['company']['locations']['branches'])

print("Marketing Dept. Projects-> ", data_dictionary['company']['departments']['marketing']['projects'])

print("competiotors-> ", data_dictionary['market_info']['competitors'])

print("Industry in ->", data_dictionary['company']['industry'])

print("Company Headquarters-> ", data_dictionary['company']['locations']['headquarters'])

# COMMAND ----------

#Q1
str1="name:John,age:34,city:New York"
dict1={}
key_value=str1.split(',')
for i in key_value:
    key,value=i.split(':')
    dict1[key]=value
print(dict1)

    

# COMMAND ----------

#Q2
str1="The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"
dict1={}
key_value=str1.split('|')
for i in key_value:
    book_title,author,year=i.split(',')
    dict1[book_title]={'author':author,'year':year}
print(dict1)

# COMMAND ----------

#Q3
names = ["Alice", "Bob", "Charlie"]
grades = ["A", "B", "C"]
dict1={}
for i,j in zip(names,grades):
    dict1[i]=j
print(dict1)

# COMMAND ----------

#Q4
stock = {"apples": 10, "oranges": 8, "bananas": 6}
sold_items = ["apples", "oranges", "apples", "bananas"]

for items in sold_items:
    if items in stock:
        stock[items]-= 1
print(stock)

# COMMAND ----------

#Q5
names = ["Alice", "Bob", "Charlie"]
ages = [25, 30, 35]
occupations = ["Engineer", "Doctor", "Artist"]
l1=[]
for i,j,k in zip(names,ages,occupations):
    l1.append({'name':i,'age':j,'occupation':k})
print(l1)

# COMMAND ----------

#Q6
text="Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. Thanks for your help!"
print("No of Questions-> ",text.count("?"))

import re
question_sentence= re.findall('[^.!?]*\?',text)       # using cap ("^") before any character,symbol is not included in sentence
print(question_sentence)

import string
punct_re= text.translate(str.maketrans('','',string.punctuation))
print("String without punctuation-> ",punct_re)

print("wordcount of you -> ",text.count('you'))

print(text.split('?')[0] + text.split('.')[-1])
