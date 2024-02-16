# #1. Convert "name:John,age:34,city:New York" string to dictionary using python. 
 
# my_str = "name:John,age:34,city:New York"

# my_split = my_str.split(',')

# print(my_split)

# my_dict ={}

# for item in my_split:
#     key,value = item.split(':')
#     my_dict[key] = value

# print(my_dict)









'''
2.
You have a string that represents information about various books. Each book's information is separated by a pipe (|), and within each book's data, different attributes like title, author, and year are separated by a comma. Your task is to convert this string into a dictionary where each book's title is the key, and the value is another dictionary containing the author and year of publication. 
 
String : "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949" 
 
Convert this string into a nested dictionary with book titles as keys and another dictionary with 'author' and 'year' as keys and their respective values. 
Expected Output: 
 
{ 

    	'The Great Gatsby': {'author': 'F. Scott Fitzgerald', 'year': '1925'}, 

  	 'To Kill a Mockingbird': {'author': 'Harper Lee', 'year': '1960'}, 

   	 '1984': {'author': 'George Orwell', 'year': '1949'} 

} 
'''

# my_str2 =  "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"

# my_str2_split =my_str2.split('|')

# print('second string split',my_str2_split)

# new_dict ={}

# for item in my_str2_split:
#     split_item = item.split(',')
#     print(split_item)
#     new_dict[split_item[0]] = {
#         'author':split_item[1],
#         'year':split_item[2]
#     }

# print(new_dict)

'''
Merge two different lists into dictionaries: 
names = ["Alice", "Bob", "Charlie"] 
grades = ["A", "B", "C"] 

'''
# names = ["Alice", "Bob", "Charlie"] 
# grades = ["A", "B", "C"] 
# #approach 1
# my_zip = zip(names,grades)
# print(dict(my_zip))
# #approach 2
# my_dict3 ={}
# for key,value in zip(names,grades):
#     my_dict3[key] = value
    
# print(my_dict3)

"""
You have a dictionary of stock quantities for various items and a list of items that have been sold. Update the dictionary by reducing the stock quantity for each sold item. 
 
stock = {"apples": 10, "oranges": 8, "bananas": 6} 
sold_items = ["apples", "oranges", "apples", "bananas"]

"""

# stock = {"apples": 10, "oranges": 8, "bananas": 6} 
# sold_items = ["apples", "oranges", "apples", "bananas"]

# for sold_item in sold_items:
#     if sold_item in stock:
#         stock[sold_item]-=1

# print(stock)

"""
You have three lists, containing names, ages, and occupations of a group of people. Combine these lists into a list of dictionaries, each dictionary containing 'name', 'age', and 'occupation' as keys. 
List: 
names = ["Alice", "Bob", "Charlie"] 
ages = [25, 30, 35] 
occupations = ["Engineer", "Doctor", "Artist"] 

"""
# names = ["Alice", "Bob", "Charlie"] 
# ages = [25, 30, 35] 
# occupations = ["Engineer", "Doctor", "Artist"] 

# my_list = []
# for item in zip(names,ages,occupations):
#     name_value,age_value,occupation_value = item
#     print(item)
#     temp_dict={
#         'name':name_value,
#         'age':age_value,
#         'occupation':occupation_value
#     }
#     my_list.append(temp_dict)

# print(my_list)


"""
You have received a text message that contains various sentences, some of which are inquiries (ending with a question mark '?'). Your task is to analyze this message and perform several operations using Python string functions. 
 
Text :  "Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. Thanks for your help!" 
 
Count the Questions: Determine how many sentences in the text are questions. 
Extract Questions: Create a list containing only the sentences that are questions. 
Remove Punctuation: Create a new version of the text with all punctuation removed. 
Word Count: Count how many times the word 'you' appears in the text (case-insensitive). 
Concatenate: Concatenate the first and last sentences to form a new sentence. 
"""
text_str = "Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. Thanks for your help!" 

#count the questions

# question_count = text_str.count("?")

# print(question_count)

#extract questions

# question_list = []
# for sentence in text_str.split('.'):
#     print('my sentence',sentence)
#     if '?' in sentence:
#         new_question = sentence[0:sentence.index('?') + 1]
#         question_list.append(new_question)

# print(question_list)

# #apprach 2 using list comprehension
# question_list =[sentence[0:sentence.index('?') + 1]  for sentence in text_str.split('.') if '?' in sentence]

# print(question_list)

# #remove puncuation
# punction_list = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
# no_puncuation_list =""
# for letter in text_str:
#     if letter in punction_list:
#         no_puncuation_list = text_str.replace(letter,'')

# print('old str with puncuation',text_str)
# print('new str without puncuation',no_puncuation_list)  


#word count

# you_count = text_str.lower().count('you')

# print('you count',you_count)

#concatenate
#concatenate first and last sentence to form new sentence

new_str = ''

new_strlist =[]
for sentence in text_str.split('.'):
    new_strlist.append(sentence)
new_str = new_strlist[0]+'.' + new_strlist[-1]
print(new_strlist)
print(new_str)

