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

print('Name of the company: ', data_dictionary['company']['name'])

print('Number of employees in the development department: ', data_dictionary['company']['departments']['development']['employees'])

print('Emerging market trend: ', data_dictionary['market_info']['market_trends']['emerging'])

print('Branch locations of company: ', data_dictionary['company']['locations']['branches'])

print('Projects under the marketing departments: ', data_dictionary['company']['departments']['marketing']['projects'])

print('Competitors of the company: ', data_dictionary['market_info']['competitors'])

print('Company headquarter is located in: ', data_dictionary['company']['locations']['headquarters'])

print('The company is in {} industry'.format(data_dictionary['company']['industry']))