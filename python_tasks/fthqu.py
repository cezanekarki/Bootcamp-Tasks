#5
names = ["Alice", "Bob", "Charlie"] 
ages = [25, 30, 35] 
occupations = ["Engineer", "Doctor", "Artist"]
 
people_list = [{'Name': name, 'Age': age, 'Occupation': occupation} for name, age, occupation in zip(names, ages, occupations)]
 
print(people_list)