def analyze_text(text):
    # Split the text into words
    words = text.split()

    # Number of words and unique words
    num_words = len(words)
    unique_words = set(words)
    num_unique_words = len(unique_words)

    # Frequency of each word
    word_freq = {}
    for word in words:
        if word in word_freq:
            word_freq[word] += 1
        else:
            word_freq[word] = 1

    # Longest word
    longest_word = max(words, key=len)

    # List of words in alphabetical and reverse alphabetical order
    # words_alphabetical = sorted(words, key=str.lower)
    # words_reverse_alphabetical = sorted(words, key=str.lower, reverse=True)
    words_alphabetical = sorted(words, key=lambda x: x.lower())
    words_reverse_alphabetical = sorted(words, key=lambda x: x.lower(), reverse=True)


    # Reversed version of the original text
    # reversed_text = (words[::-1])
    reversed_text = ' '.join(words[::-1])


    # Displaying the analysis
    print("Number of words:", num_words)
    print("Number of unique words:", num_unique_words)
    print("Frequency of each word:")
    for word, freq in word_freq.items():
        print(word, "-", freq)
    print("Longest word:", longest_word)
    print("Words in alphabetical order:", words_alphabetical)
    print("Words in reverse alphabetical order:", words_reverse_alphabetical)
    print("Reversed version of the original text:")
    print(reversed_text)

def main():
    text = input("Enter a text:", )
    analyze_text(text)

if __name__ == '__main__':
    main()



# FIRST APPROACH:::::::::::

input_text = input("Enter a string: ")

# 1. calculate number of words and unique words
words = input_text.split()
num_words = len(words)
# Use the set() class to convert the list to a set.
unique_words = set(words)
unique_words_length = len(unique_words)

# 2. Calculate the frequency of each word using a dictionary
word_frequency = {}
for word in words:
    if word in word_frequency:
        word_frequency[word] += 1
    else:
        word_frequency[word] = 1

# 3. Find the longest word in the text
longest_word = max(words)

# 4. Create a list of all words in alphabetical order and reverse alphabetical order
words_alphabetical = sorted(words)
words_reverse_alphabetical = sorted(words, reverse=True)

words_alphabetical = sorted(words, key=str.lower)
words_reverse_alphabetical = sorted(words, key=str.lower, reverse=True)


# 5. Create a reversed version of the original text
reversed_text = input_text[::-1]

# Print
print("Frequency of each word:")
for word, freq in word_frequency.items():
    print(f"{word}: {freq}")
print("Number of words:", num_words)
print("Number of unique words:", unique_words_length)

print("Longest word in the text:", longest_word)
print("Words in alphabetical order:", words_alphabetical)
print("Words in reverse alphabetical order:", words_reverse_alphabetical)
print("Reversed version of the original text:", reversed_text)