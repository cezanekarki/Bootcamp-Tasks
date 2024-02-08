# Databricks notebook source
import requests


s = requests.Session()


data = {"key": "value"}
response_post = s.post("https://gitlab.com/gitlab-org/gitlab-docs", json=data)

if response_post.status_code == 200:
    print('POST Request Successful')

    data_response = response_post.json()

    # Update headers with the token
    s.headers.update({"Authorization": f"Bearer {data_response['token']}"})

    # Example GET request
    response_get = s.get("https://gitlab.com/gitlab-org/gitlab-docs")

    # Check the status code and print the content if the request was successful (status code 200)
    if response_get.status_code == 200:
        print('GET Request Successful')
        print(response_get.text)
    else:
        print(f'GET Request Failed. Status code: {response_get.status_code}')

else:
    print(f'POST Request Failed. Status code: {response_post.status_code}')

# Close the session
s.close()


# COMMAND ----------

import requests

# Replace these with your GitLab settings
gitlab_base_url = "https://gitlab.com"
project_id = "gitlab-org/gitlab-docs"
private_token = "glpat-Ezna-vuEBtJouWQcTxqR" 

# Set up headers with private token
headers = {"PRIVATE-TOKEN": private_token}

# Make a request to get the list of files
url = f"{gitlab_base_url}/api/v4/projects/{project_id}/repository/tree"
params = {"recursive": "true"}
response = requests.get(url, headers=headers, params=params)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    files = response.json()

    # Print the list of files
    print("List of Files:")
    for file in files:
        print(file["path"])

else:
    print(f"Request Failed. Status code: {response.status_code}")
    print(response.text)


# COMMAND ----------

import requests
import json
# Replace 'your_private_token' with your GitLab personal access token
private_token = 'glpat-BS5sLBkZszdTgq84RQbm'
 
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
    
    formatted = json.dumps(files, indent=2)
    print(formatted)
    # Display the list of file names
    for file in files:
        print(file['name'])
else:
    # Print an error message if the request was not successful
    print(f"Error: {response.status_code}, {response.text}")
 


# COMMAND ----------

import requests 
import json 
gitlab_url = "https://gitlab.com"
repo_path = "gitlab-org/gitlab-docs"
token = "glpat-BS5sLBkZszdTgq84RQbm"
 
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

from datetime import datetime

# Get the current timestamp
current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

# Create the filename using the current timestamp
filename = f"repo-query-{current_timestamp}.json"

# Print the filename
print(filename)

# COMMAND ----------

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
access_token = 'glpat-BS5sLBkZszdTgq84RQbm'
 
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
 

# COMMAND ----------

import requests
import json
from datetime import datetime
 
# API endpoints
base_url = 'https://gitlab.com/api/v4/projects/{project_id}'
repository_tree_url = base_url + '/repository/tree'
contributors_url = base_url + '/repository/contributors'
 
#  project ID
project_id = '1794617'
 
# personal access token
access_token = 'glpat-BS5sLBkZszdTgq84RQbm'
 
# Parameters for the request
params = {
    'private_token': access_token,  
}
 
try:
    # Fetching contributors
    response = requests.get(contributors_url.format(project_id=project_id), params=params)
    contributors = []
    response.raise_for_status()  # Raise an exception for HTTP errors (status code >= 400)
    contributors_data = response.json()
    for contributor in contributors_data:
        contributors.append({'name': contributor['name'], 'email': contributor['email']})
 
    # Fetching repository tree
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
 
    # output dictionary
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
 
    #To  Get current timestamp in the specified format
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
 
    # Construct the filename
    filename = f'repo-query-{timestamp}.json'
 
    # Saving the output to the file
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

with open('repo-query-20240207052723.json', 'r') as f:
    data = json.load(f)
formatted_data = json.dumps(data, indent=2)
print(formatted_data)

# COMMAND ----------


