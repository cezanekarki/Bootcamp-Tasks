# -*- coding: utf-8 -*-
"""TechkraftPython.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/18peipq7GodrOk9mupd3ooU5IQFGM4I05
"""

string="name:John,age:34,city:New York"
info = string.split(",")
userdict= {}

for pair in info:
  key,value = pair.split(":")
  userdict[key] = str(value)

print(userdict["name"])

String = "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"
books_info = String.split("|")
print(books_info)
books_dict ={}

for pair in books_info:
  title, author, year = pair.split(",")
  books_dict[title] = {"author": author, "year" :year}


print(books_dict)

names = ["Alice", "Bob", "Charlie"]
grades = ["A", "B", "C"]

merged_dict= {}

for i in range(len(names)):

    merged_dict[names[i]] = grades[i ]



print(merged_dict)

"""Merge two different lists into dictionaries:
names = [&quot;Alice&quot;, &quot;Bob&quot;, &quot;Charlie&quot;]
grades = [&quot;A&quot;, &quot;B&quot;, &quot;C&quot;]
"""

stock = {"apples": 10, "oranges": 8, "bananas": 6}
sold_items = ["apples", "oranges", "apples", "bananas", "apples"]

for key in sold_items:
  if key in stock.keys():
    stock[key]-=1
print(stock)

names = ["Alice", "Bob", "Charlie"]
ages = [25, 30, 35]
occupations = ["Engineer", "Doctor", "Artist"]
user =[]
dicti ={}

for i in range(len(names)):
  dicti ={
      "names" : names[i],
      "ages" : ages[i],
      "occupation" : occupations[i]
  }
  user.append(dicti)

print(user)

import re
Text = "Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. Thanks for your help!"
txt_list = re.split(r'([,.!?]+)', Text)


count =Text.count("you")

no_punct_buffer = re.split(r'[,.!?]+', Text)
no_punct = ' '.join(no_punct_buffer)
question =[]
buffer =[]
for i in txt_list:

  if i == '?':

    question.append(buffer.pop())

  buffer.append(i)

print(txt_list)

conc = txt_list[0] + txt_list[-3]

print(f"The number of questions are: {len(question)} \n The questions are: {question} \n Sentence Without Punctuation is : {no_punct} \n You appears {count} times. \n The concatenated answer is : {conc}" )

students = [

    {"name": "Alice", "age": 25, "score": 92},

    {"name": "Bob", "age": 22, "score": 85},

    {"name": "Charlie", "age": 27, "score": 78},

    {"name": "David", "age": 23, "score": 95},

    {"name": "Eva", "age": 24, "score": 88}

]


for i in range(1,len(students)):
  if students[i]["age"] < students[i-1]["age"]:
    temp =students[i]
    students[i]= students[i-1]
    students[i-1] = temp


print(students)

numbers = [1,2,3,4,5,6,7,8,9,10]
print(list(filter(lambda x:x%2==0,numbers)))

"""BOOK APPLICATION PROJECT"""

