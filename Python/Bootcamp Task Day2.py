# Databricks notebook source
input_string = "name:John,age:34,city:New York"

keyvalue = input_string.split(',')


print(keyvalue)
dictionary = {}
for pair in keyvalue:
    key, value = pair.split(':')
    dictionary[key] = value

print(dictionary)

# COMMAND ----------

# book_info_string = "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"


# books = book_info_string.split(' | ')


# books_dict = {}

# # Process each book entry and populate the dictionary
# for book in books:

#     title, author, year = [info.strip() for info in book.split(',')]

#     book_dict = {'author': author, 'year': year}

#     books_dict[title] = book_dict


# print(books_dict)





book_info_string = "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"

# Split the string into individual book entries and process each entry
books_dict = {title: {'author': author, 'year': year} 
              for title, author, year in (book.split(', ') 
                                          for book in book_info_string.split(' | '))}



# COMMAND ----------

keys =  ["Alice", "Bob", "Charlie"]
values =  [2, 3, 5]
 

# dictionary = {}
# for key in keys:
#     for value in values:
#         dictionary[key] = value
#         values.remove(value)
#         break
 
# # Printing resultant dictionary

zippedObject = dict(zip(keys, values))
print(zippedObject)

# COMMAND ----------

stock = {"apples": 10, "oranges": 8, "bananas": 6}
sold_items = ["apples", "oranges", "apples", "bananas"]

# Update stock quantities based on sold items
for item in sold_items:
    if item in stock:
        stock[item] -= 1  # Reduce the stock quantity by 1 for each sold item

# Print the updated stock dictionary
print(stock)


# COMMAND ----------

names =  ["Alice", "Bob", "Charlie"]
grades =  [2, 3, 5]
 

# dictionary = {}
# for key in keys:
#     for value in values:
#         dictionary[key] = value
#         values.remove(value)
#         break
 
# # Printing resultant dictionary

zippedObject = dict(zip(names, grades))
print(zippedObject)

# COMMAND ----------

names = ["Alice", "Bob", "Charlie"]
ages = [25, 30, 35]
occupations = ["Engineer", "Doctor", "Artist"]

# Combine lists into a list of dictionaries
people = [{'name': name, 'age': age, 'occupation': occupation} for name, age, occupation in zip(names, ages, occupations)]

# Print the list of dictionaries
print(people)

# COMMAND ----------

text_string=  "Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. Thanks for your help!"
text_string = text_string.lower();
words = text_string.split(" ");  
print(words)  


# COMMAND ----------


