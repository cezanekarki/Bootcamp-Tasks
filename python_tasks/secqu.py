string="The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"
pipeSplitData=string.split('|')
dict={}
 
for item in pipeSplitData:
    parts = item.split(', ')
    title = parts[0]
    author = parts[1]
    year = parts[2].strip()  
 
    dict[title] = {'author': author, 'year': year}
 
print(dict)