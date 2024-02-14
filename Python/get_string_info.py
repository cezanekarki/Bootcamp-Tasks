def perform_task(text):
    words = text.split()

    num_words = len(words)
    unique_words = set(words)
    num_unique_words = len(unique_words)

    word_frequency = {}
    for word in words:
        word_frequency[word] = words.count(word)

    longest_word = max(words, key=len)

    words_alphabetical = sorted(words)
    words_reverse_alphabetical = sorted(words, reverse=True)

    reversed_text = text[::-1]

    print("Number of words:", num_words)
    print("Number of unique words:", num_unique_words)
    print("Frequency of each word:", word_frequency)
    print("Longest word:", longest_word)
    print("Words in alphabetical order:", words_alphabetical)
    print("Words in reverse alphabetical order:", words_reverse_alphabetical)
    print("Reversed version of the original text:", reversed_text)


user_input = input("Enter a string: ")
perform_task(user_input)
