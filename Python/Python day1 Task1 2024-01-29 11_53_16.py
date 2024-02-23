# Databricks notebook source


# COMMAND ----------



text1 = input()
list1 = text1.split()
print(len(list1))
set1 = set(list1) 
print(len(set1))



# Count the frequency of each word
dict1 = {i:list1.count(i) for i in list1}
print(dict1)
# word frequencies
for word, freq in word_freq.items():
    print(f"Word: {word}, Frequency: {freq}")
longest = max(list1, key=len)

print(f"The longest word is: {longest}")
#alphabetical order
alpha_order = sorted(list1)
print(alpha_order)
#reversed alphabetical order
reverse = alpha_order[::-1]
print(reverse)

    




# COMMAND ----------


