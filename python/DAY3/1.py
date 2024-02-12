# 1. Convert "name:John,age:34, city: New York" string to dictionary using python.

input_data =  "name:John,age:34,city:New York"
keyvaluepair = input_data.split(',')
print(keyvaluepair)

input_data_dict = {}
for i in keyvaluepair:
    key, value = i.split(':')
    # print("KEY:", key)
    # print("VALUE:", value)

    input_data_dict[key] = value

print(input_data_dict)