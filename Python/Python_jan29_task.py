user_input = input("Enter input to perform the operations ")
user_input_list = user_input.split(",")

print(user_input_list)

# Count total of words
total_count = len(user_input.split())
print(f"Total no of word are: {total_count}.")

# Count total unique no of words
unique_count = set(user_input.split())
print(f"Total unique words: {len(unique_count)}")

#----------------------------------------------------------------

#Count frequency of word using dictoinary
def count_frequency(user_input_list):
    frequency = {}
    for items in user_input_list:
        frequency[items] = user_input_list.count(items)
    print(frequency)

count_frequency(user_input_list)

#----------------------------------------------------------------

# Longest Word in a string
longest_word = max(user_input_list, key=len)
print(longest_word)

#---------------------------------------------------------------

Arrage word is alphabetical order
user_input_list.sort()

print(user_input_list)

# Arrange word in reverse alphabetical order

reversed_list = user_input_list.reverse()
print(user_input_list)


print(type(reversed_list))
print(type(user_input_list))
#--------------------------------------------------------------

# Reverse version of original text

newString=""
for i in range (len(myString)-1,-1,-1):
  newString += myString[i]
print(newString)

print(myString[::-1])