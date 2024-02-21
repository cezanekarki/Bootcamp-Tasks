# Asking for user input
#sentence = input("Enter a sentence for string manipulation: ")
sentence = "Apple is a very very rich company"
#print(sentence)

#Number of words
sentence_split = sentence.split()
num_of_words = len(sentence_split)
#print(num_of_words)
print("The number of words in the sentence is ", num_of_words)

#Unique Words
num_of_unique_words = len(set(sentence_split))
print("The number of unique words in the sentence is", num_of_unique_words)

#Frequency of each word
frequency_count = dict()

for word in sentence_split:
    if word in frequency_count:
        frequency_count[word] += 1
    else:
        frequency_count[word] = 1

print(frequency_count)

#longest word in sentence
largest_word = max(sentence_split, key=len)
print("The longest word in the sentence is: ", largest_word)


##alphabetical and Reverse Alphabetical
alphabetical_order = " ".join(sorted(sentence_split))
print("Alphabetical Order: ", alphabetical_order)

reversed_alphabetical_order = " ".join(sorted(sentence_split, reverse=True))
print("Reversed Alphabetical Order: ", reversed_alphabetical_order)

#Reversed version of origina text

reversed_original_text = sentence[::-1]
print(reversed_original_text)
