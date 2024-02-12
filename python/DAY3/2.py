# 2. You have a string that represents information about various books. Each book's information is separated by a pipe (|), and 
# within each book's data, different attributes like title, author, and year are separated by a comma. Your task is to convert 
# this string into a dictionary where each book's title is the key, and the value is another dictionary containing the author and 
# year of publication. 
 
# String : "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949" 
 
# Convert this string into a nested dictionary with book titles as keys and another dictionary with 'author' and 'year' as keys and their respective values. 
# Expected Output: 
 
# { 

#     	'The Great Gatsby': {'author': 'F. Scott Fitzgerald', 'year': '1925'}, 

#   	 'To Kill a Mockingbird': {'author': 'Harper Lee', 'year': '1960'}, 

#    	 '1984': {'author': 'George Orwell', 'year': '1949'} 

# } 

# SOLUTION:

# SECOND APPROACH:::::::::::::::::::::::::::::

import json
book_string = "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"

books = {}
for book_detail in book_string.split(" | "):
    # print('book_detail: ', book_detail)
    title, author, year = book_detail.split(", ")
    books[title] = {'author': author, 'year': year}

    # Convert dictionary to formatted string with indentation
    formatted_books = json.dumps(books, indent=3)

print(formatted_books)

# FIRST APPROACH:::::::::::::::::::::::::::::::::

input_String = "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"

books = {}

first_cleaned_data = input_String.split(" | ")
# print('first_cleaned_data: ', first_cleaned_data)

for item in first_cleaned_data:
    # Split each item by comma to separate title, author, and year
    details = item.split(", ")
    print('details: ', details)
    nested_dict = {}
    nested_dict['author'] = details[1]
    nested_dict['year'] = details[2]
    
    # Assign title as key and nested dictionary as value in books dictionary
    books[details[0]] = nested_dict

print(books)