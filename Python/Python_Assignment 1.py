# Databricks notebook source
#No. of words
string1=input()
l1=string1.split()
print(len(l1))

#unique words
set1=set(l1)
print(set1)

#frequency of each word
dict1={i:l1.count(i) for i in l1}
print(dict1)

#longest word
long_word=sorted(l1,key=len)
print(long_word[-1])

#alphabetical order
alpha_order=sorted(l1)
print(alpha_order)

alpha_order_rev=alpha_order[::-1]
print(alpha_order_rev)

#reversed version
l2=string1[::-1]
print(''.join(l2))









# COMMAND ----------

students = [
    {"name": "Alice", "age": 25, "score": 92},
    {"name": "Bob", "age": 22, "score": 85},
    {"name": "Charlie", "age": 27, "score": 78},
    {"name": "David", "age": 23, "score": 95},
    {"name": "Eva", "age": 24, "score": 88}
]
sort = sorted(students, key=lambda x: x["age"])
print(sort)



# COMMAND ----------

#filter out the even numbers from the list.
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
even_no=list(filter(lambda x:x%2==0,numbers))
print(even_no)

# COMMAND ----------

students = [
    {"name": "Alice", "score": 85},
    {"name": "Bob", "score": 92},
    {"name": "Charlie", "score": 78},
    {"name": "David", "score": 95},
    {"name": "Eve", "score": 88}
]
rev_dict=sorted(students,key=lambda student:student['score'],reverse=True)
print(rev_dict)

# COMMAND ----------


