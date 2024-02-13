string =  'my name is nabin. im now here at techkraft'

count_of_words = len(string.split())
print(f'Number of words present in your sentence is: {count_of_words}')

def freq(string):
    sep = string.split()
    string2 = []
    
    for i in sep:
        if i not in string2:
            string2.append(i)
    
    for i in range(0, len(string2)):
        print('Frequence of', string2[i], 'is: ', sep.count(string2[i]))

def main():
    string
    freq(string)               