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

# 1
print(data_dictionary["company"]["name"])

# 2
print(data_dictionary["company"]["departments"]["development"]["employees"])

# 3
print(data_dictionary["market_info"]["market_trends"]["emerging"])

# 4
print(data_dictionary["company"]["locations"]["branches"])

# 5 
print(data_dictionary["company"]["departments"]["marketing"]["projects"])

# 6
print(data_dictionary["market_info"]["competitors"])

# 7 
print(data_dictionary["company"]["industry"])

# 8
print(data_dictionary["company"]["locations"]["headquarters"])