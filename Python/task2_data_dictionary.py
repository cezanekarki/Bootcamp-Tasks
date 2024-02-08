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


# 1- what is the name of the company?
company_name = data_dictionary["company"]["name"]
print(f"Company Name: {company_name}\n")

#2 - How many employees are working in the development department?
development_dept_employees = data_dictionary["company"]["departments"]["development"]["employees"]
print(f"Employees in Development Department: {development_dept_employees}\n")


#3 - What are the emerging market trends according to this data?
market_trends = data_dictionary["market_info"]["market_trends"]
print(f"Emerging Market Trends: {market_trends}\n")

#4 - List all branch locations of the company.
branch_locations = data_dictionary["company"]["locations"]["branches"]
print(f"Branch Locations: {branch_locations}\n")

#5 - What are the projects under the marketing department?
marketing_projects = data_dictionary["company"]["departments"]["marketing"]["projects"] 
print(f"Marketing Projects: {marketing_projects}\n")

#6 - Name a competitor of the company.
competitors = data_dictionary["market_info"]["competitors"]
print(f"Competitors: {competitors}\n")

#7 - What Industry is the company in?
industry = data_dictionary["company"]["industry"]
print(f"Industry: {industry}\n")

#8 - What is the company's headquarters located?
headquarters = data_dictionary["company"]["locations"]["headquarters"]
print(f"Headquarters: {headquarters}\n")

