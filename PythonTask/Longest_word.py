string = 'Hello there. How is everyone there'
sep = string.split()
count_of_words = len(sep) 
print(f'Number of words present in your sentence is: {count_of_words}')

def freq(string):
    sep = string.split()
    string2 = []
    
    for i in sep:
        if i not in string2:
            string2.append(i)
    
    for i in range(0, len(string2)):
        print('Frequency of', string2[i], 'is: ', sep.count(string2[i]))

freq(string)                    

unique_words = set(sep)
print('Unique words: ', unique_words)

print('The longest word is: ', max(sep, key=len))
