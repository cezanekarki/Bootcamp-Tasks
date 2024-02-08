#Question1
og_string = "name:John,age:34,city:New York"

og_string_dict = dict(subString.split(":") for subString in og_string.split(","))
print(og_string_dict)

#Question2
book_string = "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"
splitlist = book_string.split("|")

book_dict = dict()
for div in splitlist:
    key, author, year = div.split(",")
    book_dict[key] = {'author': author, 'year': year}
    
print(book_dict)


#Question3
names = ["Alice", "Bob", "Charlie"] 
grades = ["A", "B", "C"]

#assigns corresponding values
zip_dict = dict(zip(names,grades))
print(zip_dict)


#Reducing stock
counts= {}
stock = {"apples": 10, "oranges": 8, "bananas": 6} 
sold_items = ["apples", "oranges", "apples", "bananas"] 

for item in sold_items:
    if item in counts:
        counts[item] += 1
    else:
        counts[item] = 1

left_stock = {key: stock[key] - counts[key] for key in set(counts) & set(stock)}
print(left_stock)


#Combine 3
names = ["Alice", "Bob", "Charlie"]
ages = [25, 30, 35]
occupations = ["Engineer", "Doctor", "Artist"]

three_nest_dict_zip = []

for name, age, occupation in zip(names, ages, occupations):
    three_nest_dict_zip.append({'name': name, 'age': age, 'occupation': occupation})

print(three_nest_dict_zip)


#From Sentence

text = "Hello! How are you? I hope you're enjoying your day. Have you seen my notebook? It's important. Thanks for your help!" 

questions_count = text.count("?")
print(questions_count)
sentences = []

# List of punctuation marks
punctuation_marks = {'.', '!', '?'}

# Initialize
splits = []
current_split = ''

# Loop through each character in the text
for char in text:
    # Check if the character is a punctuation mark
    if char in punctuation_marks:
        # Add the current segment with punctuation to the segments list
        if current_split:
            splits.append(current_split.strip() + char)
            current_split = ''
            #print(segments)
    else:
        # If the character is not a punctuation mark, add it to the current segment
        current_split += char

# Add the last segment if any
if current_split:
    splits.append(current_split.strip())
#print(segments)

splits_with_punct = splits

# Filter segments to extract questions
questions = [part for part in splits if part.strip().endswith('?')]

# Print each question
for question in questions:
    print(question)


splits = []
current_split = ''

for char in text:
    # Check if the character is a punctuation mark
    if char in punctuation_marks:
        # Add the current segment with punctuation to the segments list
        if current_split:
            splits.append(current_split.strip())
            current_split = ''
            #print(segments)
    else:
        # If the character is not a punctuation mark, add it to the current segment
        current_split += char

# Add the last segment if any
if current_split:
    splits.append(current_split.strip())

no_punct = ""
for part in splits:
    no_punct = no_punct + " " + part
    no_punct_strip = no_punct.strip()
print(no_punct_strip)

lower_text = no_punct_strip.lower()
split_lower_text = lower_text.split()

you_count = split_lower_text.count("you")

print(you_count)

concact_first_last = splits_with_punct[0] + " " + splits_with_punct[-1]
print(concact_first_last)

