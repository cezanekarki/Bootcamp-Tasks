import json
import requests 

gitlab_api_url = 'https://gitlab.com/api/v4'
gitlab_project_id = '1794617'
gitlab_access_token = 'glpat-v6Jx7TiMn-TyxKsGx7zU'

headers = {
        'PRIVATE-TOKEN': gitlab_access_token,
    }

def get_projects():
    url = f'{gitlab_api_url}/projects'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Error: {response.status_code}')
        print(response.text)
        return None 
    
def get_project_tree(project_id):
    url = f'{gitlab_api_url}/projects/{project_id}/repository/tree'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Error: {response.status_code}')
        print(response.text)   
        return None
    
projects = get_projects()
contributors = get_project_contributors()
project_tree = get_project_tree()

formatted_contributors= [{'name': value['name'], 'email': value['email']} for value in contributors]

 
folders = [{'folder_name': value['name'], 'folder_path': value['path'], 'folder_mode': value['mode']} for value in project_tree if value['type'] == 'tree']
files = [{'file_name': value['name'], 'file_path': value['path'], 'file_mode': value['mode']} for value in project_tree if value['type'] == 'blob']

if project_tree:
    for item in project_tree:
        print(item['name'], item['type'])
        if item['type'] == 'tree':
            print(item['name'], item['path'], item['mode'])
        elif item['type'] == 'blob':
            print(item['name'], item['path'], item['mode'])
 
my_dict = {
    "id": project['id'],
    "name": project['name'],
    "contributors": formatted_contributors,
    "contents": {
        "meta": {
            "folders_count": len(folders),
            "files_count": len(files)
        },
        "folders": folders,
        "files": files
    }
}
 
print(json.dumps(obj = my_dict, indent= 2))