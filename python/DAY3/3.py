# 3. Merge two different lists into dictionaries: 
# names = ["Alice", "Bob", "Charlie"] 
# grades = ["A", "B", "C"] 

# SOLUTION:
names = ["Alice", "Bob", "Charlie"] 
grades = ["A", "B", "C"] 

# SECOND SOLUTION:
grades_dict = {name: grade for name, grade in zip(names, grades) }
print('List comprehension: ', grades_dict)

# FIRST SOLUTION:

grades_dict = {}
for i in names:
    for j in grades:
        grades_dict[i] = j
print(grades_dict)