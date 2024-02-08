#Sort the students list of dictionaries based on the age of the students
students = [
    {"name": "Alice", "age": 25, "score": 92},
    {"name": "Bob", "age": 22, "score": 85},
    {"name": "Charlie", "age": 27, "score": 78},
    {"name": "David", "age": 23, "score": 95},
    {"name": "Eva", "age": 24, "score": 88}
]

print(sorted(students, key=lambda student: student['age']))

# You have a list of numbers:
# Using a lambda function and the filter() function, filter out the even numbers from the list. Provide the code to achieve this.
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(list(filter(lambda x: (x % 2 == 0), numbers)) )

# Reverse Sort the list of students according to the score.
students = [
    {"name": "Alice", "score": 85},
    {"name": "Bob", "score": 92},
    {"name": "Charlie", "score": 78},
    {"name": "David", "score": 95},
    {"name": "Eve", "score": 88}
]

print(sorted(students,key=lambda x: x["score"],reverse=True))




# Convert "name:John,age:34,city:New York" string to dictionary using python. 

keyPairString = "name:John,age:34,city:New York"
str_dict = {}
data = keyPairString.split(",")
for i in data:
    key = i[:i.index(":")]
    value = i[i.index(":")+1:]
    
    str_dict[key] = value

print(str_dict)


# You have a string that represents information about various books. Each book's information is separated by 
#a pipe (|), and within each book's data, different attributes like title, author, and year are separated by a 
#comma. Your task is to convert this string into a dictionary where each book's title is the key, and the value is 
#another dictionary containing the author and year of publication. 
 

"""
{ 

    'The Great Gatsby': {'author': 'F. Scott Fitzgerald', 'year': '1925'}, 

   	'To Kill a Mockingbird': {'author': 'Harper Lee', 'year': '1960'}, 

   	'1984': {'author': 'George Orwell', 'year': '1949'} 

 } 
"""
BookString = "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, \
Harper Lee, 1960 | 1984,George Orwell,1949"


book_dict ={}
book = BookString.split("|")

for items in book:
    title,author,year = items.split(",")
    book_dict[title] = {'author':author,'year':year}
    
print(book_dict)



# Merge two different lists into dictionaries: 

names = ["Alice", "Bob", "Charlie"] 
grades = ["A", "B", "C"] 

dict_merge = {}
for i in range(len(names)):
    dict_merge[names[i]] = grades[i]

# Easy way out
res = dict(zip(names, grades))
print(dict_merge)

print(res)


# You have a dictionary of stock quantities for various items and a list of items that have been sold. 
#Update the dictionary by reducing the stock quantity for each sold item. 
 
stock = {"apples": 10, "oranges": 8, "bananas": 6} 
sold_items = ["apples", "oranges", "apples", "bananas"] 

for items in sold_items:
    if items in stock.keys():
        stock[items] -= 1

print(stock)


# You have three lists, containing names, ages, and occupations of a group of people. 
# Combine these lists into a list of dictionaries, each dictionary containing 'name', 'age', and 'occupation' as keys. 
# List: 

names = ["Alice", "Bob", "Charlie"] 
ages = [25, 30, 35] 
occupations = ["Engineer", "Doctor", "Artist"] 

dict_list = []

for i in range(len(names)):
    dict_key  = {
        'name':names[i],
        'age':ages[i],
        'occupation':occupations[i]
    }
    dict_list.append(dict_key)
print(dict_list)



# You have received a text message that contains various sentences, some of which are inquiries 
#(ending with a question mark '?'). Your task is to analyze this message and perform several operations 
#using Python string functions. 

# Count the Questions: Determine how many sentences in the text are questions. 
# Extract Questions: Create a list containing only the sentences that are questions. 
# Remove Punctuation: Create a new version of the text with all punctuation removed. 
# Word Count: Count how many times the word 'you' appears in the text (case-insensitive). 
# Concatenate: Concatenate the first and last sentences to form a new sentence. 

import string

text =  "Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. \
Thanks for your help!" 

sentences = []
current_sentence = ""

for char in text:
    current_sentence += char
    if char in ".!?":
        sentences.append(current_sentence.strip())
        current_sentence = ''
if current_sentence:
    sentences.append(current_sentence.strip())

questions = [sentence for sentence in sentences if sentence.endswith('?')]

print(f"The list of questions are:{questions}")
print(sentences)

# Simple way to do 
print(f"Total number of questions are: {text.count('?')}")

# Other way is count the array
print(f"Total number of questions are: {len(questions)}")

# Text without punctuation
text_without_punctuation = text.translate(str.maketrans("", "", string.punctuation)).lower()

print("Text without punctuation:", text_without_punctuation)

# Count the number of you in text
print(f"The total number of you in text is:{text.count('you')}")

first_sentence = sentences[0].split('!')[0]
last_sentence = sentences[0].split('!')[0] if sentences[-1].endswith('!') else sentences[-1]
first_last_sentences = first_sentence + last_sentence

print(f"The concatinated sentences are: {first_last_sentences}")