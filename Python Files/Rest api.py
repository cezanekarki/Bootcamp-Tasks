# Databricks notebook source
#declare the token and id at first
private_token = 'glpat-xdzDsuC2HgypQyziHw9z'
project_id = '1794617'

# COMMAND ----------

#for printing the first page of files and folders only.
import requests
 
p = {
    "recursive": True,
    "private_token": private_token,
    "per_page": 100  
}
 
url = f'https://gitlab.com/api/v4/projects/{project_id}/repository/tree'
 
response = requests.get(url, params = p)
 
if response.status_code == 200:
    repository_tree = response.json()
    print(repository_tree)
 
    #for item in repository_tree:
     #   print(item['name'])
else:
    print(f"Failed to retrieve repository tree. Status code: {response.status_code}")
    print(response.text)
 

# COMMAND ----------

#For contributors name and id
url1 = f"https://gitlab.com/api/v4/projects/{project_id}/repository/contributors"
headers = {'Private-Token': private_token}

response = requests.get(url1, headers=headers)

if response.status_code == 200:
    try:
        contributors = response.json()

        for contributor in contributors:
            name = contributor.get('name', 'N/A')
            email = contributor.get('email', 'N/A')
            print(f"Contributor Name: {name}, Email: {email}")
    except ValueError as e:
        print(f"Error decoding JSON: {e}")
        print(response.text)
else:
    print(f"Failed to retrieve contributors. Status code: {response.status_code}")
    print(response.text)


# COMMAND ----------

#folder count and file count
page = 1
per_page = 100
total_folders = 0
total_files = 0

while True:
    params = {
        "recursive": True,
        "private_token": private_token,
        "per_page": per_page,
        "page": page
    }

    url = f'https://gitlab.com/api/v4/projects/{project_id}/repository/tree'

    response = requests.get(url, params=params)

    if response.status_code == 200:
        try:
            repository_tree = response.json()

            folder_count = sum(1 for item in repository_tree if item['type'] == 'tree')
            file_count = sum(1 for item in repository_tree if item['type'] == 'blob')

            total_folders += folder_count
            total_files += file_count

            if 'next' in response.links:
                page += 1
            else:
                break  

        except ValueError as e:
            print(f"Error decoding JSON: {e}")
            print(response.text)
            break  
    else:
        print(f"Failed to retrieve repository tree. Status code: {response.status_code}")
        print(response.text)
        break  
print(f"Total number of folders: {total_folders}")
print(f"Total number of files: {total_files}")


# COMMAND ----------

#folder name path and mode
params = {
    "recursive": "true",
    "private_token": private_token,
    "per_page": 100
}

url = f'https://gitlab.com/api/v4/projects/{project_id}/repository/tree'

response = requests.get(url, params=params)

if response.status_code == 200:
    try:
        repository_tree = response.json()

        folders = []
        files = []

        for item in repository_tree:
            if item['type'] == 'tree':
                folder_details = {
                    "folder_name": item['name'],
                    "folder_path": item['path'],
                    "folder_mode": item['mode']
                }
                folders.append(folder_details)
            elif item['type'] == 'blob':
                file_details = {
                    "file_name": item['name'],
                    "file_path": item['path'],
                    "file_mode": item['mode']
                }
                files.append(file_details)

        print("Folders:")
        for folder in folders:
            print(folder)

        print("\nFiles:")
        for file in files:
            print(file)

    except ValueError as e:
        print(f"Error decoding JSON: {e}")
        print(response.text)
else:
    print(f"Failed to retrieve repository tree. Status code: {response.status_code}")
    print(response.text)


# COMMAND ----------

#project name and id
{
    "id": "project id",
    "name": "<<project name>>",
    "contributors": [
        {
            "name": "<<contributor name>>"
            "email": "<<contributor email>>"
        },
        ...
    ],
    "contents": {
        "meta": {
            "folders_count": <<folders_count>>,
            "files_count": <<files_count>>
        },
        "folders": [
            {
                "folder_name": "<<folder_name>>",
                "folder_path": "<<folder_path>>",
                "folder_mode": "<<folder_mode>>"
            },
            ...
        ],
        "files": [
            {
                "file_name": "<<file_name>>",
                "file_path": "<<file_path>>",
                "file_mode": "<<file_mode>>"
            },
            ...
        ]
    }
}

# COMMAND ----------

url = f'https://gitlab.com/api/v4/projects/{project_id}'
params = {
    "private_token": private_token,
}

response = requests.get(url, params=params)

if response.status_code == 200:
    try:
        project_data = response.json()

        project_id = project_data.get('id', 'N/A')
        project_name = project_data.get('name', 'N/A')

        print(f"Project ID: {project_id}")
        print(f"Project Name: {project_name}")

    except ValueError as e:
        print(f"Error decoding JSON: {e}")
        print(response.text)
else:
    print(f"Failed to retrieve project data. Status code: {response.status_code}")
    print(response.text)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


