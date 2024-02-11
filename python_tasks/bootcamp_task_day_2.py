'''
1. Convert "name:John,age:34,city:New York" string to 
dictionary using python.
'''
def string_dict():
    string_val = "name:John,age:34,city:New York" 

    temp_list = string_val.split(",")

    dictionary = dict()
    for index , value  in  [x.split(":") for x in temp_list]:
        dictionary[index] = value
    print(dictionary)

'''
2.	You have a string that represents information about various books. Each book's information is separated by a pipe (|), and within each book's data, different attributes like title, author, and year are separated by a comma. Your task is to convert this string into a dictionary where each book's title is the key, and the value is another dictionary containing the author and year of publication.

String : "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"

Convert this string into a nested dictionary with book titles as keys and another dictionary with 'author' and 'year' as keys and their respective values.
Expected Output:

{
    	'The Great Gatsby': {'author': 'F. Scott Fitzgerald', 'year': '1925'},
  	 'To Kill a Mockingbird': {'author': 'Harper Lee', 'year': '1960'},
   	 '1984': {'author': 'George Orwell', 'year': '1949'}
}
'''
   
def map_dict():
    string_val = "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"

    temp_list = string_val.split("|")

    print('temp_list', temp_list)
    dictionary = dict()

    for i in temp_list:
        
        book_attributes = i.split(', ')
        # Extract title, author, and year
        title = book_attributes[0]
        author = book_attributes[1]
        year = book_attributes[2]
        
        book_dict = {'author': author, 'year': year}
        
        dictionary[title] = book_dict

    print('dictionary: ', dictionary)

'''
3.	Merge two different lists into dictionaries:
names = ["Alice", "Bob", "Charlie"]
grades = ["A", "B", "C"]
'''
# string_dict()
def merge_list():
    names = ["Alice", "Bob", "Charlie"]
    grades = ["A", "B", "C"]
    # print(list(zip(names, grades)))
    res = {}
    for key in grades:
        for value in names:
            res[key] = value
            names.remove(value)
            break
    print("Resultant dictionary is : " + str(res))

'''
4.	You have a dictionary of stock quantities for various items and a list of items that have been sold. Update the dictionary by reducing the stock quantity for each sold item.

stock = {"apples": 10, "oranges": 8, "bananas": 6}
sold_items = ["apples", "oranges", "apples", "bananas"]
'''
def update_dict():
    stock = {"apples": 10, "oranges": 8, "bananas": 6}
    sold_items = ["apples", "oranges", "apples", "bananas"]
    sold_item_dict = check_frequency(sold_items)
    print('sold_item_dict', sold_item_dict)

    updated_stock = dict()
    #check if key of the both dict is same if same subtract value of that key 
    for key1, value1 in stock.items():
        for key2, value2 in sold_item_dict.items():
            if key1 == key2:
                newVal = value1 - value2
                # print ("key1", newVal)
                updated_stock[key1] = newVal

    print('updated stock', updated_stock)

  
def check_frequency(lst):
    dict = {}
    for i in lst:
        if (i in dict):
            dict[i] += 1
        else:
            dict[i] = 1
    
    print(dict)
    return dict

'''
5.	You have three lists, containing names, ages, and occupations of a group of people. Combine these lists into a list of dictionaries, each dictionary containing 'name', 'age', and 'occupation' as keys.
List:
names = ["Alice", "Bob", "Charlie"]
ages = [25, 30, 35]
occupations = ["Engineer", "Doctor", "Artist"]
'''
def combine_three_list():
    names = ["Alice", "Bob", "Charlie"]
    ages = [25, 30, 35]
    occupations = ["Engineer", "Doctor", "Artist"]

    # key_list = ["names", "ages", "occupations"]

    people_list = [{'name': name, 'age': age, 'occupation': occupation} for name, age, occupation in zip(names, ages, occupations)]
    print(people_list)

'''
6.	You have received a text message that contains various sentences, some of which are inquiries (ending with a question mark '?'). Your task is to analyze this message and perform several operations using Python string functions.

Text :  "Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. Thanks for your help!"

Count the Questions: Determine how many sentences in the text are questions.
Extract Questions: Create a list containing only the sentences that are questions.
Remove Punctuation: Create a new version of the text with all punctuation removed.
Word Count: Count how many times the word 'you' appears in the text (case-insensitive).
Concatenate: Concatenate the first and last sentences to form a new sentence.

'''
import string
def string_manipulation():
   
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
    
    text_no_punctuation = ''.join(char for char in text if char not in string.punctuation)
    you_count = text.lower().count('you')
    first_sentence = sentences[0].split('!')[0]
    last_sentence = sentences[-1].split('!')[0] if sentences[-1].endswith('!') else sentences[-1]
    first_and_last_sentence = first_sentence + ' ' + last_sentence
    
    question_count = len(questions)
    print('question_count', question_count)
    print('questions', questions)
    print('text_no_punctuation', text_no_punctuation)
    print('you_count', you_count)
    print('first_and_last_sentence', first_and_last_sentence)
    concatenation_sentence = sentences[0].split('!')[0] +' '+ sentences[-1].split('!')[0]
    print('concatenation_sentence', concatenation_sentence)



#uncomment the function to test

# merge_list()
# map_dict()
# update_dict()
# combine_three_list()   
# string_manipulation()
# sort_dict()
