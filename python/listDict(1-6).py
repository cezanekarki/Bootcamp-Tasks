# # import json
# # my_string= '{ "name":"John","age":"34","city":"New York" }'
# # dictionary_from_json = json.loads(my_string)
# # print(dictionary_from_json )

# #...............111......................................

# my_string= "name:John,age:34,city:New York"
# split_string= my_string.split(',')
# print(split_string)
# new_dict ={}
# for item in split_string:
#     key, value = item.split(':')
#     new_dict[key] = value
#     print(new_dict) 
#     print(type(new_dict))
#............2222............................

# demo_book = "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"

# splitted_book_detail = demo_book.split("|")

# print(splitted_book_detail)

# my_dict = {}

# for item in splitted_book_detail:
#     split_item = item.split(",")

#     my_dict[split_item[0]] = {
#         'author':split_item[1],
#         'year':split_item[2]
#     }
# print(my_dict) 

#...............333......................
# names = ["Alice", "Bob", "Charlie"] 
# grades = ["A", "B", "C"] 

# my_dict = dict(zip(names ,grades))
# print(my_dict)

#..........................444...

 
# stock = {"apples": 10, "oranges": 8, "bananas": 6} 
# sold_items = ["apples", "oranges", "apples", "bananas"]

# for item in sold_items:
#     if item in stock:
#         stock[item]-=1

# print(stock)        

#.........[{}{}{}].....................5...........

# names = ["Alice", "Bob", "Charlie"] 
# ages = [25, 30, 35] 
# occupations = ["Engineer", "Doctor", "Artist"] 
 
# my_dict = zip(names ,ages, occupations)
# new_list=[]

# for item in my_dict: 
#     name,age,accupations=item
#     print( name,age,accupations)

#     new_dict = {
#         'name':name,
#         'age':age,
#         'occupations':occupations

#     }
#     new_list.append(new_dict)
# print(new_list)    

#.......................6...........

Text =  "Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. Thanks for your help!"
count=0
for item in Text:
    if item=="?":
        count += 1
print(" Total No of questions=",count)

question_list=[]


    

