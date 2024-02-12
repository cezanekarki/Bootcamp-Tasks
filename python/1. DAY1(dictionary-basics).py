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

# 1. What is the name of the company?
# company = data_dictionary["company"]
# print('company Dict: ', company)
company_name = data_dictionary["company"]["name"]
# print('company_name: ', company_name)


# 2. How many employees are in the development department?
development_employees = data_dictionary["company"]["departments"]["development"]["employees"]

# 3. What are the emerging market trends according to this data?
emerging_market_trend = data_dictionary["market_info"]["market_trends"]["emerging"]

# 4. List all branch locations of the company.
branch_locations = data_dictionary["company"]["locations"]["branches"]

# 5. What are the projects under the marketing department?
marketing_projects = data_dictionary["company"]["departments"]["marketing"]["projects"]

# 6. Name a competitor of the company.
competitor = data_dictionary["market_info"]["competitors"][0]  # suru ko euta matra lina

# 7. What industry is the company in?
industry = data_dictionary["company"]["industry"]

# 8. Where is the company's headquarters located?
headquarters_location = data_dictionary["company"]["locations"]["headquarters"]

# Now printing all the answers:
print("1. Company Name:", company_name)
print("2. Employees in Development Department:", development_employees)
print("3. Emerging Market Trends:", emerging_market_trend)
print("4. Branch Locations:", branch_locations)
print("5. Projects under Marketing Department:", marketing_projects)
print("6. Competitor:", competitor)
print("7. Industry:", industry)
print("8. Headquarters Location:", headquarters_location)
