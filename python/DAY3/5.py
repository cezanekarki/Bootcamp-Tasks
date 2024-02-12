# 5. You have three lists, containing names, ages, and occupations of a group of people. Combine these lists into a list of dictionaries, each dictionary containing 'name', 'age', and 'occupation' as keys. 
# List:
# names = ["Alice", "Bob", "Charlie"] 
# ages = [25, 30, 35] 
# occupations = ["Engineer", "Doctor", "Artist"]

names = ["Alice", "Bob", "Charlie"] 
ages = [25, 30, 35] 
occupations = ["Engineer", "Doctor", "Artist"]

# SECOND APPROACH:::
# Combine the lists into a list of dictionaries
person = [{'name':name, 'age': age, 'occupation': occupation} for name, age, occupation in zip(names, ages, occupations)]
print("Person: ", person)


# FIRST APPROACH:::
data_details = []
for i in range(len(names)):
    data = {}
    data['name'] = names[i]
    data['age'] = ages[i]
    data['occupation'] = occupations[i]
    data_details.append(data)

print("Data Details: ", data_details)