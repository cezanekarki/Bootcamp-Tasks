private_token = 'glpat-tydJKZUVH_n12wrdwMwc'
project_id = '1794617'



import requests


params = {
    'recursive': 'true',
    'per_page':100000
}
url = f'https://gitlab.com/api/v4/projects/{project_id}/repository/tree'
headers = {
    'Content-Type': 'application/json',
    'PRIVATE-TOKEN': private_token
}

url2 =f'https://gitlab.com/api/v4/projects/{project_id}/repository/contributors'


response2=requests.get(url2,headers=headers,params=params)

if response2.status_code == 200:
 
    repository_tree = response2.json()
    for item in repository_tree:
                print("name:",item["name"] +" | "+ " email:",item["email"])



else:
    
    print(f"Failed to retrieve repository tree. Status code: {response2.status_code}")
    print(response2.text)

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
 
    repository_tree = response.json()
    for item in repository_tree:
        print("name:",item["name"] +" | "+ " path:",item["path"])


else:
   
    print(f"Failed to retrieve repository tree. Status code: {response.status_code}")
    print(response.text)




import requests

private_token = 'glpat-tydJKZUVH_n12wrdwMwc'
project_id = '1794617'
params = {
    'recursive': 'true',
    'per_page': 100000
}


contributors_url = f'https://gitlab.com/api/v4/projects/{project_id}/repository/contributors'
headers = {
    'Content-Type': 'application/json',
    'PRIVATE-TOKEN': private_token
}
contributors_Data = requests.get(contributors_url, headers=headers, params=params)


project_data = {
    "id": project_id,
    "name": "<<project name>>",
    "contributors": [],
    "contents": {
        "meta": {
            "folders_count": 0,
            "files_count": 0
        },
        "folders": [],
        "files": []
    }
}


if contributors_Data.status_code == 200:
    contributors_data = contributors_Data.json()
    for contributor in contributors_data:
        contributor_data = {
            "name": contributor.get("name", ""),
            "email": contributor.get("email", "")
        }
        project_data["contributors"].append(contributor_data)

repository_tree_url = f'https://gitlab.com/api/v4/projects/{project_id}/repository/tree'
response_repository_tree = requests.get(repository_tree_url, headers=headers, params=params)


if response_repository_tree.status_code == 200:
    repository_tree_data = response_repository_tree.json()
    
    for item in repository_tree_data:
        if 'type' in item:
            if item['type'] == 'tree':  # It's a folder
                folder_data = {
                    "folder_name": item["name"],
                    "folder_path": item["path"],
                    "folder_mode": item["mode"]
                }
                project_data["contents"]["folders"].append(folder_data)
                project_data["contents"]["meta"]["folders_count"] += 1
            elif item['type'] == 'blob':  # It's a file
                file_data = {
                    "file_name": item["name"],
                    "file_path": item["path"],
                    "file_mode": item["mode"]
                }
                project_data["contents"]["files"].append(file_data)
                project_data["contents"]["meta"]["files_count"] += 1

   
    print(project_data["contents"])

else:
    print(f"Failed to retrieve repository tree. Status code: {response_repository_tree.status_code}")
    print(response_repository_tree)




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



import requests

private_token = 'glpat-tydJKZUVH_n12wrdwMwc'
project_id = '1794617'
params = {
    'recursive': 'true',
    'per_page': 100000
}


contributors_url = f'https://gitlab.com/api/v4/projects/{project_id}/repository/contributors'
headers = {
    'Content-Type': 'application/json',
    'PRIVATE-TOKEN': private_token
}
contributors_Data = requests.get(contributors_url, headers=headers, params=params)


project_data = {
    "id": project_id,
    "name": "<<project name>>",
    "contributors": [],
    "contents": {
        "meta": {
            "folders_count": 0,
            "files_count": 0
        },
        "folders": [],
        "files": []
    }
}

if contributors_Data.status_code == 200:
    contributors_data = contributors_Data.json()
    for contributor in contributors_data:
        contributor_data = {
            "name": contributor.get("name", ""),
            "email": contributor.get("email", "")
        }
        project_data["contributors"].append(contributor_data)

    while 'next' in contributors_Data.links:
        next_url_contributors = contributors_Data.links['next']['url']
        contributors_Data = requests.get(next_url_contributors, headers=headers)
        if contributors_Data.status_code == 200:
            contributors_data = contributors_Data.json()
            for contributor in contributors_data:
                contributor_data = {
                    "name": contributor.get("name", ""),
                    "email": contributor.get("email", "")
                }
                project_data["contributors"].append(contributor_data)
        else:
            print(f"Failed to retrieve contributors data. Status code: {contributors_Data.status_code}")
            print(contributors_Data)

repository_tree_url = f'https://gitlab.com/api/v4/projects/{project_id}/repository/tree'
response_repository_tree = requests.get(repository_tree_url, headers=headers, params=params)


if response_repository_tree.status_code == 200:
    repository_tree_data = response_repository_tree.json()
    
    for item in repository_tree_data:
        if 'type' in item:
            if item['type'] == 'tree':  
                folder_data = {
                    "folder_name": item["name"],
                    "folder_path": item["path"],
                    "folder_mode": item["mode"]
                }
                project_data["contents"]["folders"].append(folder_data)
                project_data["contents"]["meta"]["folders_count"] += 1
            elif item['type'] == 'blob':  
                file_data = {
                    "file_name": item["name"],
                    "file_path": item["path"],
                    "file_mode": item["mode"]
                }
                project_data["contents"]["files"].append(file_data)
                project_data["contents"]["meta"]["files_count"] += 1

    while 'next' in response_repository_tree.links:
        next_url_tree = response_repository_tree.links['next']['url']
        response_repository_tree = requests.get(next_url_tree, headers=headers)
        if response_repository_tree.status_code == 200:
            repository_tree_data = response_repository_tree.json()
            
            for item in repository_tree_data:
                if 'type' in item:
                    if item['type'] == 'tree':  
                        folder_data = {
                            "folder_name": item["name"],
                            "folder_path": item["path"],
                            "folder_mode": item["mode"]
                        }
                        project_data["contents"]["folders"].append(folder_data)
                        project_data["contents"]["meta"]["folders_count"] += 1
                    elif item['type'] == 'blob': 
                        file_data = {
                            "file_name": item["name"],
                            "file_path": item["path"],
                            "file_mode": item["mode"]
                        }
                        project_data["contents"]["files"].append(file_data)
                        project_data["contents"]["meta"]["files_count"] += 1
            print(project_data["contents"])

        else:
            print(f"Failed to retrieve repository tree data. Status code: {response_repository_tree.status_code}")
            print(response_repository_tree)

else:
    print(f"Failed to retrieve repository tree. Status code: {response_repository_tree.status_code}")
    print(response_repository_tree)


