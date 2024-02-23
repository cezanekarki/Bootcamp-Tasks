# Databricks notebook source
#without contributor-1


import requests
 
# Replace 'your_private_token' with your GitLab personal access token
private_token = 'glpat-8KFNcmr2yViNfQYhNa6j'
 
# API endpoint to get a list of repository files (replace 'main' with the desired branch)
api_url = 'https://gitlab.com/api/v4/projects/gitlab-org%2Fgitlab-docs/repository/tree'
 
# Parameters for the API request
params = {'private_token': private_token, 'ref': 'main'}
 
# Make the API request
response = requests.get(api_url, params=params)
# print(response.text)
 
# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    files = response.json()  # Use response.json() here
 
    # Display the list of file names
    for file in files:
        print(file['name'])
else:
    # Print an error message if the request was not successful
    print(f"Error: {response.status_code}, {response.text}")
 

# COMMAND ----------

#With contributor first way
import requests
import json
 
gitlab_url = "https://gitlab.com"
repo_path = "gitlab-org/gitlab-docs"
token = "glpat-8KFNcmr2yViNfQYhNa6j"
 
api_url = f"{gitlab_url}/api/v4/projects/{repo_path.replace('/', '%2F')}/repository/tree?recursive=true"
headers = {"PRIVATE-TOKEN": token}
 
response = requests.get(api_url, headers=headers)
 
if response.status_code == 200:
    project_info = response.json()
    # Use json.dumps with indent parameter for pretty printing
    formatted_json = json.dumps(project_info, indent=2)
    print(formatted_json)
else:
    print(f"Error: {response.status_code} - {response.text}")
 

# COMMAND ----------


#with contributor whole final code
import requests
import json
from datetime import datetime
 
# Define the GitLab API endpoints
base_url = 'https://gitlab.com/api/v4/projects/{project_id}'
repository_tree_url = base_url + '/repository/tree'
contributors_url = base_url + '/repository/contributors'
 
# Define the project ID
project_id = '1794617'
 
# Define your personal access token
access_token = 'glpat-8KFNcmr2yViNfQYhNa6j'
 
# Parameters for the request
params = {
    'private_token': access_token,  # Use your personal access token
}
 
# Fetch contributors
response = requests.get(contributors_url.format(project_id=project_id), params=params)
contributors = []
if response.status_code == 200:
    contributors_data = response.json()
    for contributor in contributors_data:
        contributors.append({'name': contributor['name'], 'email': contributor['email']})
else:
    print('Failed to retrieve contributors:', response.status_code, response.text)
 
# Fetch repository tree
response = requests.get(repository_tree_url.format(project_id=project_id), params=params)
if response.status_code == 200:
    repository_data = response.json()
    files = []
    folders = []
    for item in repository_data:
        if item['type'] == 'blob':
            files.append({'file_name': item['name'], 'file_path': item['path'], 'file_mode': item['mode']})
        elif item['type'] == 'tree':
            folders.append({'folder_name': item['name'], 'folder_path': item['path'], 'folder_mode': item['mode']})
else:
    print('Failed to retrieve repository tree:', response.status_code, response.text)
 
# Construct the output dictionary
output = {
    'id': project_id,
    'name': 'gitlab-docs',
    'contributors': contributors,
    'contents': {
        'meta': {
            'folders_count': len(folders),
            'files_count': len(files)
        },
        'folders': folders,
        'files': files
    }
}
 
# Get current timestamp in the specified format
timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
 
# Construct the filename
filename = f'repo-query-{timestamp}.json'
 
# Save the output to the file
with open(filename, 'w') as file:
    json.dump(output, file, indent=4)
 
print(f"Result saved to {filename}")
 
#has context menu


# COMMAND ----------


#to load the data 
import os
os.listdir()

with open('repo-query-20240207051016.json', 'r') as f:
    data = json.load(f)
formatted_data = json.dumps(data, indent=2)
print(formatted_data)
 

# COMMAND ----------

#with contributor using functions
import requests
import json
from datetime import datetime

# Define constants
BASE_URL = 'https://gitlab.com/api/v4/projects/{project_id}'
REPOSITORY_TREE_URL = BASE_URL + '/repository/tree'
CONTRIBUTORS_URL = BASE_URL + '/repository/contributors'

def fetch_data(url, params):
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise ValueError(f"Failed to fetch data from {url}: {response.status_code} - {response.text}")

def save_to_file(filename, data):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)

def main(project_id, access_token):
    params = {'private_token': 'glpat-8KFNcmr2yViNfQYhNa6j'}
    
    # Fetch contributors
    contributors_data = fetch_data(CONTRIBUTORS_URL.format(project_id=project_id), params)
    contributors = [{'name': c['name'], 'email': c['email']} for c in contributors_data]
    
    # Fetch repository tree
    repository_data = fetch_data(REPOSITORY_TREE_URL.format(project_id=project_id), params)
    files = [{'file_name': item['name'], 'file_path': item['path'], 'file_mode': item['mode']} 
             for item in repository_data if item['type'] == 'blob']
    folders = [{'folder_name': item['name'], 'folder_path': item['path'], 'folder_mode': item['mode']} 
               for item in repository_data if item['type'] == 'tree']

    # Construct the output dictionary
    output = {
        'id': project_id,
        'name': 'gitlab-docs',
        'contributors': contributors,
        'contents': {
            'meta': {
                'folders_count': len(folders),
                'files_count': len(files)
            },
            'folders': folders,
            'files': files
        }
    }

    # Get current timestamp in the specified format
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    
    # Construct the filename
    filename = f'repo-query-{timestamp}.json'
    
    # Save the output to the file
    save_to_file(filename, output)
    print(f"Result saved to {filename}")

if __name__ == "__main__":
    project_id = '1794617'  # Enter your project ID
    access_token = 'glpat-8KFNcmr2yViNfQYhNa6j'  # Enter your access token
    main(project_id, access_token)

# COMMAND ----------


import os
os.listdir()

with open('repo-query-20240207051016.json', 'r') as f:
    data = json.load(f)
formatted_data = json.dumps(data, indent=2)
print(formatted_data)
 

# COMMAND ----------

#with contributor with try and catch block
import requests
import json
from datetime import datetime

# Define the GitLab API endpoints
base_url = 'https://gitlab.com/api/v4/projects/{project_id}'
repository_tree_url = base_url + '/repository/tree'
contributors_url = base_url + '/repository/contributors'

# Define the project ID
project_id = '1794617'

# Define your personal access token
access_token = 'glpat-8KFNcmr2yViNfQYhNa6j'

# Parameters for the request
params = {
    'private_token': access_token,  # Use your personal access token
}

try:
    # Fetch contributors
    response = requests.get(contributors_url.format(project_id=project_id), params=params)
    contributors = []
    response.raise_for_status()  # Raise an exception for HTTP errors (status code >= 400)
    contributors_data = response.json()
    for contributor in contributors_data:
        contributors.append({'name': contributor['name'], 'email': contributor['email']})

    # Fetch repository tree
    response = requests.get(repository_tree_url.format(project_id=project_id), params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors (status code >= 400)
    repository_data = response.json()
    files = []
    folders = []
    for item in repository_data:
        if item['type'] == 'blob':
            files.append({'file_name': item['name'], 'file_path': item['path'], 'file_mode': item['mode']})
        elif item['type'] == 'tree':
            folders.append({'folder_name': item['name'], 'folder_path': item['path'], 'folder_mode': item['mode']})

    # Construct the output dictionary
    output = {
        'id': project_id,
        'name': 'gitlab-docs',
        'contributors': contributors,
        'contents': {
            'meta': {
                'folders_count': len(folders),
                'files_count': len(files)
            },
            'folders': folders,
            'files': files
        }
    }

    # Get current timestamp in the specified format
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

    # Construct the filename
    filename = f'repo-query-{timestamp}.json'

    # Save the output to the file
    with open(filename, 'w') as file:
        json.dump(output, file, indent=4)

    print(f"Result saved to {filename}")

except requests.RequestException as e:
    print("Error during API request:", e)
except json.JSONDecodeError as e:
    print("Error decoding JSON response:", e)
except IOError as e:
    print("Error saving result to file:", e)

# COMMAND ----------

import os
os.listdir()

with open('repo-query-20240207052538.json', 'r') as f:
    data = json.load(f)
formatted_data = json.dumps(data, indent=2)
print(formatted_data)

# COMMAND ----------

# with contributor using classes 

