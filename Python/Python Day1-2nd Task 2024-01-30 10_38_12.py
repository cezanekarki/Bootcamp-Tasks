# Databricks notebook source
#1.	Convert "name:John,age:34,city:New York" string to dictionary using python. 
string1 ='name:John,age:34,city:New York'
split_str = string1.split(',')
print("splited string is ",split_str)
dict1 = {}
for word in split_str:
    key,value = word.split(':')

    dict1[key]= value 

print("Dictionary format is",dict1)     


# COMMAND ----------

#2.second task
string2 = "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949" 
split_str2 = string2.split('|')
print("splitted string2 is",split_str2)
dict2 = {}
for word in split_str2:
    book_title,author,year = word.split(',')
    dict2[book_title] ={"author":author,"year":year}
print("Dictionary format is",dict2)    

# COMMAND ----------

#3.	Merge two different lists into dictionaries: 
names = ["Alice", "Bob", "Charlie"] 
grades = ["A", "B", "C"] 
merge_lists = {names: grades for names, grades in zip(names, grades)}
print("merged_lists in dictionary form are",merge_lists)


 


# COMMAND ----------

#4.	You have a dictionary of stock quantities for various items and a list of items that have been sold. Update the dictionary by reducing the stock quantity for each sold item. 
stock = {"apples": 10, "oranges": 8, "bananas": 6} 
sold_items = ["apples", "oranges", "apples", "bananas"] 
for item in sold_items:
    if item in stock:
        stock[item] -=1
    else:
        print("item",{'item'}," is not in the stock" )    
print("updated list of stocks is",stock)        
 


# COMMAND ----------

#5.	You have three lists, containing names, ages, and occupations of a group of people. Combine these lists into a list of dictionaries, each dictionary containing 'name', 'age', and 'occupation' as keys. 
#List: 
names = ["Alice", "Bob", "Charlie"] 
ages = [25, 30, 35] 
occupations = ["Engineer", "Doctor", "Artist"] 
merged_lists =[{"names":names,"ages":ages,"occupations":occupations} for names,ages,occupations in zip(names,ages,occupations)]
print("merged list is",merged_lists)



# COMMAND ----------


"""6.	You have received a text message that contains various sentences, some of which are inquiries (ending with a question mark '?'). Your task is to analyze this message and perform several operations using Python string functions. 
 
Text :  "Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. Thanks for your help!" 
 
Count the Questions: Determine how many sentences in the text are questions. 
Extract Questions: Create a list containing only the sentences that are questions. 
Remove Punctuation: Create a new version of the text with all punctuation removed. 
Word Count: Count how many times the word 'you' appears in the text (case-insensitive). 
Concatenate: Concatenate the first and last sentences to form a new sentence. """
text = "Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. Thanks for your help!"

#count the questions
count_q = text.count('?')
print("number of questions are" ,count_q)
#word "you" count
count_you = text.lower().count('you')
print("number of times 'you' appear is ",count_you)
 
#concatenate first and last sentences to form a new sentences
spliting = text.split()
print (spliting)
sentences = [sentence.strip() for sentence in text.split('.') if sentence.strip()]
print(sentences)
 
 
 
 
 



# COMMAND ----------


