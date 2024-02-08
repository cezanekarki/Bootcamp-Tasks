def getData():
    userInput=input("Enter the string: ")
    userInputLength=len(userInput)
    split_words = userInput.split()
    
    print(f"The length of the string is {userInputLength} characters")
    print("original words - ",split_words)
    print("number of words - ",len(split_words))
 
    word_set = set(split_words)
    print("Unique Words - ",word_set)
    print("revered original text - ",userInput[::-1])
 
    word_dict = {word: split_words.count(word) for word in split_words}
    print("frequency of words", word_dict)
    longest_word = max(split_words, key = len)
    print("longest word",longest_word)
 
    
    alpha_words=sorted(split_words, key=len, reverse=False)
    print("alphabetical words - "," ".join(alpha_words))
    reverse_words= sorted(split_words, key=len, reverse=True)
 
    print("reverse words - "," ".join(reverse_words))
 
 
getData()