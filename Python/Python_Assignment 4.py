# Databricks notebook source
import json

string = '{"name": "John Doe", "dob": "1990-01-01 00:00:00", "city": "Anytown", "email": "john.doe@example.com", "is_student": false}'

data= json.loads(string)
print(data)


from datetime import datetime

with open('data.json','w') as json_file:
    json.dump(data,json_file)

with open('data.json','r') as json_file:
    data_loaded= json.load(json_file)

    data_loaded['dob']= datetime.strptime(data_loaded['dob'],'%Y-%m-%d %H:%M:%S')

print(data_loaded)

# COMMAND ----------


import requests
import json
from datetime import datetime
 
repository_tree_file_url = 'https://gitlab.com/api/v4/projects/{project_id}/repository/tree'
repository_tree_folder_url = 'https://gitlab.com/api/v4/projects/{project_id}/repository/tree?recursive=true'
contributors_url = 'https://gitlab.com/api/v4/projects/{project_id}/repository/contributors'
 
project_id = '1794617'
 
access_token = 'glpat-UbkHJEe2zNgqotvLPc7E'
 
params = {
    'private_token': access_token,  
}
 
response = requests.get(contributors_url.format(project_id=project_id), params=params)
contributors = []
if response.status_code == 200:
    contributors_data = response.json()
    for contributor in contributors_data:
        contributors.append({'name': contributor['name'], 'email': contributor['email']})
else:
    print('Failed to retrieve contributors:', response.status_code, response.text)
 
response = requests.get(repository_tree_file_url.format(project_id=project_id), params=params)
if response.status_code == 200:
    repository_data = response.json()
    files = []
    for item in repository_data:
        if item['type'] == 'blob':
            files.append({'file_name': item['name'], 'file_path': item['path'], 'file_mode': item['mode']})
else:
    print('Failed to retrieve repository tree:', response.status_code, response.text)


response_1 = requests.get(repository_tree_folder_url.format(project_id=project_id), params=params)
if response_1.status_code == 200:
    repository_data_1 = response_1.json()
    folders = []
    for item in repository_data_1:
        if item['type'] == 'tree':
            folders.append({'folder_name': item['name'], 'folder_path': item['path'], 'folder_mode': item['mode']})
else:
    print('Failed to retrieve repository tree:', response_1.status_code, response_1.text)

 
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
 
timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
 
filename = f'repo-query-{timestamp}.json'
 
with open(filename, 'w') as file:
    json.dump(output, file, indent=4)
 
print(f"Result saved to {filename}")



with open(filename, 'r') as f:
    data = json.load(f)
formatted_data = json.dumps(data, indent=2)
print(formatted_data)

# COMMAND ----------


