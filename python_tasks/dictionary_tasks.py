#task 1
data_dictionary = {
    "company": {
        "name": "TechGiant",
        "industry": "Technology",
        "locations": {
            "headquarters": "New York",
            "branches": ["San Francisco", "Berlin", "Tokyo"]
        },
        "departments": {
            "development": {
                "employees": 120,
                "projects": ["App Development", "Software Maintenance"]
            },
            "marketing": {
                "employees": 45,
                "projects": ["Brand Campaign", "Social Media"]
            },
            "sales": {
                "employees": 70,
                "projects": ["Domestic Sales", "International Sales"]
            }
        }
    },
    "market_info": {
        "competitors": ["DataCorp", "InnovaCon", "NextGenTech"],
        "market_trends": {
            "current": "AI and Machine Learning",
            "emerging": "Quantum Computing"
        }
    }
}

company_name = data_dictionary["company"]["name"]
no_of_dev_employees = data_dictionary["company"]["departments"]["development"]["employees"]
emerging_market_trends = data_dictionary["market_info"]["market_trends"]["emerging"]
branch_locations = data_dictionary["company"]["locations"]["branches"]
marketing_projects = data_dictionary["company"]["departments"]["marketing"]["projects"]
competitor = data_dictionary["market_info"]["competitors"]
industry = data_dictionary["company"]["industry"]
head_quarter = data_dictionary["company"]["locations"]["headquarters"]

print(company_name)
print(no_of_dev_employees)
print(emerging_market_trends)
print(branch_locations)
print(marketing_projects)
print(competitor)
print(industry)
print(head_quarter)



#task 2
info =  "name:John,age:34,city:New York"

key_value_pairs = info.split(",")
print(key_value_pairs)

result_dict = {key_value.split(":")[0]:key_value.split(":")[1] for key_value in key_value_pairs}


print(result_dict)


#task 3

book_info = "The Great Gatsby, F. Scott Fitzgerald, 1925 | To Kill a Mockingbird, Harper Lee, 1960 | 1984, George Orwell, 1949"
separate_book_info = book_info.split("|")
final_dict = {}

result_dict = {separate_book_info[i].split(",")[0]:
              {"author":separate_book_info[i].split(",")[1], 
                "year": separate_book_info[i].split(",")[2]} for i in range(len(separate_book_info)) }

#for i in separate_book_info:
    #book_name, author, year = i.split(", ")
    #final_dict[book_name] = {
        #"author":author,
        #"year":year

print(result_dict)


#task 4

names = ["Alice", "Bob", "Charlie"] 
grades = ["A", "B", "C"]

#d = {names[i]:grades[i] for i in range(len(names))}
#print(d)

d = dict(zip(names, grades))
print(d)

# task 5

names = ["Alice", "Bob", "Charlie"] 
ages = [25, 30, 35] 
occupations = ["Engineer", "Doctor", "Artist"] 

people_list = []

for i in range(len(names)):
    dict_people = {'names':names[i],
                   'ages':ages[i],
                   'occupations':occupations[i]}
    people_list.append(dict_people)

print("List:", people_list)

#task 6

