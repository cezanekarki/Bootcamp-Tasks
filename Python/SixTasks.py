#1 Convert "name:John,age:34, city: New York" string to dictionary using python. 
 
string_data="name:John,age:34, city: New York"
splitData=string_data.split(',')
details_dict={}
for data in splitData:
    key,value=data.split(':')
    details_dict[key]=value
    
print(details_dict)


##2
givenData="The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"
pipeSplitData=givenData.split('|')
# print(pipeSplitData)
books_dict={}
 
for item in pipeSplitData:
    parts = item.split(', ')
    title = parts[0]
    author = parts[1]
    year = parts[2].strip()  
 
    books_dict[title] = {'author': author, 'year': year}
 
print(books_dict)


# 3
names = ["Alice", "Bob", "Charlie"] 
grades = ["A", "B", "C"]
my_dict=dict(zip(names,grades))
print(my_dict)


#4
stock = {"apples": 10, "oranges": 8, "bananas": 6}
sold_items = ["apples", "oranges", "apples", "bananas"]
 
# Update the stock based on sold items
for item in sold_items:
    if item in stock:
        stock[item] -= 1
    else:
        print(f"{item}' not in the stock.")
 
print("Updated stock:", stock)


#5
names = ["Alice", "Bob", "Charlie"] 
ages = [25, 30, 35] 
occupations = ["Engineer", "Doctor", "Artist"]
 
people_list = [{'name': name, 'age': age, 'occupation': occupation} for name, age, occupation in zip(names, ages, occupations)]
 
print(people_list)


#6

text = "Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. Thanks for your help!"
 
 
sentences = []
current_sentence = ''
for char in text:
    current_sentence += char
    if char in '.!?':
        sentences.append(current_sentence.strip())
        current_sentence = ''
if current_sentence:
    sentences.append(current_sentence.strip())
 
#print(sentences)
questions = [sentence for sentence in sentences if sentence.endswith('?')]
 
import string
text_no_punctuation = ''.join(char for char in text if char not in string.punctuation)
you_count = text.lower().count('you')
first_sentence = sentences[0].split('!')[0]
last_sentence = sentences[-1].split('!')[0] if sentences[-1].endswith('!') else sentences[-1]
first_and_last_sentence = first_sentence + ' ' + last_sentence
 
question_count = len(questions)
 
#question_count, questions, text_no_punctuation, you_count, first_and_last_sentence
 
 
concatenation_sentence = sentences[0].split('!')[0] +' '+ sentences[-1].split('!')[0]
concatenation_sentence