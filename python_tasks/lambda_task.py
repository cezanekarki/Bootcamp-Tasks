students = [
    {"name": "Alice", "age": 25, "score": 92},
    {"name": "Bob", "age": 22, "score": 85},
    {"name": "Charlie", "age": 27, "score": 78},
    {"name": "David", "age": 23, "score": 95},
    {"name": "Eva", "age": 24, "score": 88}
]

sorted_students = sorted(students, key = lambda x : x["age"])
print(sorted_students)

def get_age(student):
    return student['age']
 
sorted_students = sorted(students, key=get_age)
print(sorted_students)


numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
#Using a lambda function and the filter() function, filter out the even numbers from the list. Provide the code to achieve this.

even_numbers = filter(lambda x : x % 2 == 0, numbers)
print(even_numbers)

students = [
    {"name": "Alice", "score": 85},
    {"name": "Bob", "score": 92},
    {"name": "Charlie", "score": 78},
    {"name": "David", "score": 95},
    {"name": "Eve", "score": 88}
]
#Reverse Sort the list of students according to the score.
#has context menu



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
 