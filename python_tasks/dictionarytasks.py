#1 Convert "name:John,age:34, city: New York" string to dictionary using python.
string = "name:John,age:34,city:New York"

dict_values = string.split(',')

dictionary = {}
for pair in dict_values:
    key, value = pair.split(':')
    dictionary[key] = value

print(dictionary)

 