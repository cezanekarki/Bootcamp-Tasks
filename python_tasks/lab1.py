def user_input(inputVal):
    val_Length = len(inputVal)
    user_input = inputVal.lower().split()
    unique = []
    print("user_input", user_input)
    for i in user_input:
        if i not in unique:
            unique.append(i)
    print("unique words",unique, "totalCount", len(unique))
    check_frequency(user_input)
    sort_data(user_input)
    reverse_lst(user_input) 
    reverse_word(inputVal)
    
def check_frequency(lst):
    dict = {}
    for i in lst:
        if (i in dict):
            dict[i] += 1
        else:
            dict[i] = 1
    print(dict)
    check_longest(dict)

def check_longest(dict):
    # longest = 0
    # for i in dict:
    #     wordLen = len(i)
    #     # print(wordLen)
    #     if longest < wordLen:
    #         longest = wordLen
    
    # print("longest",i,longest)

    longest_word = max(dict, key=len)
    word_len =len(longest_word)
    print("longest word is",longest_word, "and its length is", word_len)
            
def sort_data(lst):
    lst.sort()
    print("sorted list", lst)
    print("sorted data is "," ".join(lst))

    
def reverse_lst(lst):
    lst.sort(reverse=True)
    print("reverse list",lst)
    print("reverse data is "," ".join(lst))

def reverse_word(val):
    rev = val[::-1]
    print('reverse data: ', rev)
    
def get_user_input():
    input_data=input("Enter the string data:")
    user_input(input_data)

get_user_input()
