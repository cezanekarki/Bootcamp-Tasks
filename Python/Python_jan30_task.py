original_string = "name:John,age:34,city:New York"

spilt_string = original_string.split(',')

print(spilt_string)

my_dict = {}

for items in spilt_string:
    key, value = items.split(':')
    my_dict[key] = value

print(my_dict) 

print(type(my_dict))

#------------------------------------------------------

demo_book = "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"

splitted_book_detail = demo_book.split("|")

print(splitted_book_detail)

my_dict = {}

for item in splitted_book_detail:
    split_item = item.split(",")

    my_dict[split_item[0]] = {
        'author':split_item[1],
        'year':split_item[2]
    }
    

print('my new dict',my_dict)

#-----------------------------------------------------------------------

names = ["Alice", "Bob", "Charlie", "ggf"] 
grades = ["A", "B", "C"]

demo_dict = {}

my_dict = dict(zip(names, grades))
print(my_dict)
naya = dict(my_dict)
print('naya naya',naya)
for key,value in my_dict:
    
    demo_dict[key] = value

print(demo_dict)   


my_dict = {}

for item in names:
    for items in grades:
        my_dict[item] = items


print(my_dict)        

----------------------------------------------------------

stock = {"apples": 10, "oranges": 8, "bananas": 6} 
sold_items = ["apples", "oranges", "apples", "bananas"]

for item in sold_items:
    if item in stock:
        stock[item] -=1

print(stock)

#-------------------------------------------------------------

#[{"name":"Alice",}{}{}]

names = ["Alice", "Bob", "Charlie"] 
ages = [25, 30, 35] 
occupations = ["Engineer", "Doctor", "Artist"]

my_dict =zip(names, ages,occupations)
#print(my_dict)


my_list = []

for item in my_dict:
    print(item)
    name,age,occupations = item
    print(name,age,occupations)
    my_dict = {
        'name':name,
        'age':age,
        'occupation':occupations
    }
    my_list.append(my_dict)

print('new dict that i created',my_list)

# --------------------------------------------------------

demo_text =  "Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. Thanks for your help!"

# count=0
# for item in demo_text:
#     if item == "?":
#         count += 1
# print(count)       
# demo2 = demo_text.split(".")
# print(demo2)

sentences = []
current_sentence = ""
for char in text:
    current_sentence += char
    if char in ".!?":
        sentences.append(current_sentence.strip())
        current_sentence = ""
if current_sentence:
    sentences.append(current_sentence.strip())

questions = [sentences for sentence in sentences if sentence.endswith("?")]

#--Or

import string
text_no_punc = "".join(char for char in text if char not in string.puntuation)
you_count = text.lower().count("you")
first_sentence = sentences[0].split("!")[0]
last_sentence = sentences[-1].split("!")[0] if sentences[-1].endswith("!") else sentences[-1]
first_and_last_sentence = first_sentence + " " + last_sentence

question_count = len(questions)

question_count, questions, text_no_punc, you_count, first_and_last_sentence


