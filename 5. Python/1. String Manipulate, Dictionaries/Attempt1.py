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


if "company" in data_dictionary and "locations" in data_dictionary["company"]:
    company_locations = data_dictionary["company"].get("locations",{})
    #print(company_locations)


if "company" in data_dictionary and "departments" in data_dictionary["company"]:
    dev_dept = data_dictionary["company"]["departments"].get("development",{})
    marketing_dept = data_dictionary["company"]["departments"].get("marketing",{})
    sales_dept = data_dictionary["company"]["departments"].get("sales",{})

    #print(dev_dept)
    #print(marketing_dept)
    #print(sales_dept)

if "market_info" in data_dictionary:
    competitor_names = data_dictionary["market_info"].get("competitors",{})
    market_trends_info = data_dictionary["market_info"].get("market_trends", {})

    #print(competitors_info)
    #print(market_trends_info)


#Company Name
company_name = data_dictionary["company"].get("name")
print(f"Company Name: {company_name}")

#Employees in Dev Department
dev_dept_emp = dev_dept.get("employees")
print(f"Development Department Employees: {dev_dept_emp}")

#Emerging Market Trends
emerge_mkt_trends = market_trends_info.get("emerging")
print(f"Emerging Market Trends: {emerge_mkt_trends}")

#Brach Locations
branch_locations = company_locations.get("branches")
branch_locations_text = ", ".join(branch_locations)
print(f"Branch Locations: {branch_locations_text}")

#Marketing Projects
marketing_projects = marketing_dept.get("projects")
marketing_projects_text = ", ".join(marketing_projects)
print(f"Branch Locations: {marketing_projects_text}")


#One of the Competitors
import random
competitor_name = random.choice(competitor_names)
print(f"One of the Company Competitor: {competitor_name}")

#All of the competitors
competitor_names_text = ", ".join(competitor_names)
print(f"All Company Competitor: {competitor_names_text}")

#company Industry
company_industry = data_dictionary["company"].get("industry")
print(f"Company industry: {company_industry}")

#Company HQ
company_hq = company_locations.get("headquarters")
print(f"Company HQ: {company_hq}")
