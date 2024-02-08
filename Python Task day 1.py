# Databricks notebook source
  # myString = input("enter any string")
myString="zebra camel tiher tiher lion aliphant"

# unique word in an input
uniqueWords = set(myString.split())
print(uniqueWords)  

# length of the input
length = len(uniqueWords)
print(length)  

# longest word h
longest_word = max(uniqueWords,key=len)
print(longest_word)


# finding frequecny of each words
words = myString.split()

dct = {} 
 
for i in words: 
    dct[i]=words.count(i) 
     
print(dct) 

# sort alphabetically and reverse alphabetically 

sortedAlphabetically =sorted(words)
print(sortedAlphabetically)


sortedAlphabetically =sorted(words,reverse=True)
print(sortedAlphabetically)


# reverse the input
newString=""
for i in range (len(myString)-1,-1,-1):
  newString += myString[i]
print(newString)

print(myString[::-1])




# COMMAND ----------


