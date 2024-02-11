def main():
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

    company_name= data_dictionary["company"]["name"]
    print("company_name:", company_name)

    development_employees = data_dictionary["company"]["departments"]["development"]["employees"]
    print("development_employees:", development_employees)

    emerging_market_trend = data_dictionary["market_info"]["market_trends"]["emerging"]
    print("emerging_market_trend:", emerging_market_trend)

    branch_list = data_dictionary["company"]["locations"]["branches"]
    print("branch_list:", branch_list)

    marketing_projects = data_dictionary["company"]["departments"]["marketing"]["projects"]
    print("marketing_projects:", marketing_projects)

    competitors_list= data_dictionary["market_info"]["competitors"]
    print("competitors_list:", competitors_list)

    competitors_name = competitors_list[0]
    print("competitors_name:", competitors_name)

    company_industry= data_dictionary["company"]["industry"]
    print("company_industry:", company_industry)

    company_headquater = data_dictionary["company"]["locations"]["headquarters"]
    print("company_headquater:", company_headquater)

main()
