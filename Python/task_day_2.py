import string

#Question 1
person = "name:John,age:34,city:New York"

separate_by_comma = person.split(",")
person_dict = {}

for i in separate_by_comma:
    y = i.split(':')
    person_dict[y[0]] = y[1]
    
print('Converted dictionary is: ', person_dict)


#Question 2
book_info = "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"

book_info_separation = book_info.split('|')
book_dictionary = {}

for i in book_info_separation:
    y = i.split(',')
    nested_dict = {
        'author': y[1],
        'year': y[2]
    }
    book_dictionary[y[0]] = nested_dict

print('\nBook dictionary is: ',book_dictionary)

#Question 3
names = ["Alice", "Bob", "Charlie"] 
grades = ["A", "B", "C"] 

grade_dict = dict(zip(names, grades))


print('\nGrade dictionary is: ', grade_dict)

#Question 4
stock = {"apples": 10, "oranges": 8, "bananas": 6} 
sold_items = ["apples", "oranges", "apples", "bananas"] 

for i in sold_items:
    if i in stock:
        stock[i] -= 1

print('\nStock is: ', stock)

#Question 5
names = ["Alice", "Bob", "Charlie"] 
ages = [25, 30, 35] 
occupations = ["Engineer", "Doctor", "Artist"] 

group_of_people = []
x = zip(names, ages, occupations)
for value in x:
    a = {}
    a['name'] = value[0]
    a['age'] = value[1]
    a['occupation'] = value[2]
    group_of_people.append(a)

print('\nGroup of people is: ', group_of_people)


# Question 6
text = "Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. Thanks for your help!"

question_mark_count = text.count('?')

print('\nNumber of questions: ', question_mark_count)


list_of_sentence = []
i = 0
for index, v in enumerate(text):
    if (v == '!' or v == '?' or v == '.'):
        list_of_sentence.append(text[i:index + 1].lstrip(' '))
        i = index + 1

questions = [i for i in list_of_sentence if '?' in i]


print('\nExtracted questions are: ', questions)

removed_punctuation_text = ''.join(a for a in text if a not in string.punctuation)
print('\nText after removing punctuation: ', removed_punctuation_text)

lowercase_text = text.lower()
print(lowercase_text.count('you'))

concatenated_result = list_of_sentence[0] + ' ' + list_of_sentence[-1]

print('\nConcatenated result: ',concatenated_result)
