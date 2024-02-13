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

questions = [sentence for sentence in sentences if sentence.endswith('?')]

import string
text_no_punctuation = ''.join(char for char in text if char not in string.punctuation)
you_count = text.lower().count('you')
first_sentence = sentences[0].split('!')[0]
last_sentence = sentences[-1].split('!')[0] if sentences[-1].endswith('!') else sentences[-1]
first_and_last_sentence = first_sentence + ' ' + last_sentence

question_count = len(questions)

concatenation_sentence = sentences[0].split('!')[0] +' '+ sentences[-1].split('!')[0]
print("Question Count:", question_count)
print("Questions:", questions)
print("Text without Punctuation:", text_no_punctuation)
print("Occurrences of 'you':", you_count)
print("First and Last Sentence:", first_and_last_sentence)
print("Concatenation Sentence:", concatenation_sentence)
