# Databricks notebook source



#Here's the dictionary, please find the questions in the attached image and complete them.
 
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

# Access the name of the company
company_name = data_dictionary["company"]["name"]

print("Name of the company:", company_name)


# Access the number of employees in the development department
development_employees = data_dictionary["company"]["departments"]["development"]["employees"]

print("Number of employees in the development department:", development_employees)

# Access the emerging data trend
emerging_trend = data_dictionary["market_info"]["market_trends"]["emerging"]

print("Emerging data trend according to the dictionary:", emerging_trend)

# Access the list of branches
branches = data_dictionary["company"]["locations"]["branches"]

print("List of branches of the company:")
for branch in branches:
    print(branch)

# Access the projects under the marketing department
marketing_projects = data_dictionary["company"]["departments"]["marketing"]["projects"]

print("Projects under the marketing department:")
for project in marketing_projects:
    print(project)

# Access the list of competitors
competitors = data_dictionary["market_info"]["competitors"]

print("Competitors of the company:")
for competitor in competitors:
    print(competitor)

# Access the industry in which the company operates
industry = data_dictionary["company"]["industry"]

print("Industry of the company:", industry)

# Access the location of the company's headquarters
headquarters_location = data_dictionary["company"]["locations"]["headquarters"]

print("Location of the company's headquarters:", headquarters_location)



# COMMAND ----------


