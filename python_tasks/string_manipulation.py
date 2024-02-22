#task 1
text = "The end is the beginning is the end"
t = text.lower()
words = t.split(" ")
total_words = len(words)
unique_words = len(set(words))
order = " ".join(sorted(words))
rev_order =" ".join(sorted(words, reverse = True))
reverse = text[::-1]
longest_word = max(words, key = len)
print(reverse)
print("Total words:", total_words)
print("Unique words:", unique_words)
print(order)
print(rev_order)
print("Longest Word:",longest_word)

#task 2
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
 
 
text_no_punctuation = ''.join(char for char in text if char not in string.punctuation)
first_sentence = sentences[0].split('!')[0]
last_sentence = sentences[-1].split('!')[0] if sentences[-1].endswith('!') else sentences[-1]
first_and_last_sentence = first_sentence + ' ' + last_sentence
print(f"The list of questions are:{questions}")
print(sentences)

# Simple way to do 
print(f"Total number of questions are: {text.count('?')}")

# Other way is count the array
print(f"Total number of questions are: {len(questions)}")

# Text without punctuation
print("Text without punctuation:", text_no_punctuation)

# Count the number of you in text
print(f"The total number of you in text is:{text.count('you')}")

first_last_sentences = first_sentence + last_sentence

print(f"The concatinated sentences are: {first_last_sentences}")


 




