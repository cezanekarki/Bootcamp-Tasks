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

# What is the name of the company?
company_name = data_dictionary["company"]["name"]
print(f"Company Name:{company_name}")

# How many employee are in the development department?
development_employees = data_dictionary["company"]["departments"]["development"]["employees"]
print(f"Development Department Employees:{development_employees}")

# What are the emerging maket trends according to this data?
emerging_trends = data_dictionary["market_info"]["market_trends"]["emerging"]
print(f"Emerging Market Trends: {emerging_trends}")

# List all the branch location of the company?
brand_location = data_dictionary["company"]["locations"]["branches"]
print(f"Branch location of the company: {brand_location}")

# What are the projects under the marketing department?
marketing_projects = data_dictionary["company"]["departments"]["marketing"]["projects"]
print(f"Marketing Department Projects:{marketing_projects}")

# Name a competitor of the company?
competitor_name = data_dictionary["market_info"]["competitors"]
print(f"Competitor Name:{competitor_name}")

# What industry is the company in?
industry = data_dictionary["company"]["industry"]
print(f"Industry:{industry}")

# 8. Where is the company's headquarters located?
headquarters_location = data_dictionary["company"]["locations"]["headquarters"]
print(f"Headquarters Location: {headquarters_location}")

