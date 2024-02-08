
##1
givenData="The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"
pipeSplitData=givenData.split('|')
# print(pipeSplitData)
books_dict={}
 
for item in pipeSplitData:
    parts = item.split(', ')
    title = parts[0]
    author = parts[1]
    year = parts[2].strip()  
 
    books_dict[title] = {'author': author, 'year': year}
 
print(books_dict)

#

# 3
names = ["Alice", "Bob", "Charlie"] 
grades = ["A", "B", "C"]
my_dict=dict(zip(names,grades))
print(my_dict)


#4
stock = {"apples": 10, "oranges": 8, "bananas": 6}
sold_items = ["apples", "oranges", "apples", "bananas"]
 
# Update the stock based on sold items
for item in sold_items:
    if item in stock:
        stock[item] -= 1
    else:
        print(f"{item}' not in the stock.")
 
print("Updated stock:", stock)


#5
names = ["Alice", "Bob", "Charlie"] 
ages = [25, 30, 35] 
occupations = ["Engineer", "Doctor", "Artist"]
 
people_list = [{'name': name, 'age': age, 'occupation': occupation} for name, age, occupation in zip(names, ages, occupations)]
 
print(people_list)


#6

