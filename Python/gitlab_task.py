import requests
import json

gitlab_api_url = 'https://gitlab.com/api/v4'
gitlab_project_id = '1794617'
gitlab_access_token = 'glpat-uwSi-st4dNJVZ1L4PfcZ'
headers = {
    'PRIVATE-TOKEN': gitlab_access_token,
}

def extract_next_page_url(link_header):
    links = link_header.split(', ')
    for link in links:
        if 'rel="next"' in link:
            return link.split(';')[0][1:-1]
    return None

def get_project():

    url = f'{gitlab_api_url}/projects/{gitlab_project_id}'

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f'Error: {response.status_code}')
        print(response.text)
        return None
    
def get_project_contributors():

    url = f'{gitlab_api_url}/projects/{gitlab_project_id}/repository/contributors'

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f'Error: {response.status_code}')
        print(response.text)
        return None

def get_project_tree():
    params = {
        'recursive': 'true',
        'per_page': '100',
    }
    url = f'{gitlab_api_url}/projects/{gitlab_project_id}/repository/tree'
    
    responses = []
    
    #Extract data recursively
    while url:
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            responses.extend(data)
            link_header = response.headers.get('Link', '')
            # Check if there is another page
            next_page_url = extract_next_page_url(link_header)

            if next_page_url:
                url = next_page_url
            else:
                url = None
        else:
            print(f'Error: {response.status_code}')
            print(response.text)
            return None

    return responses

project = get_project()
contributors = get_project_contributors()
project_tree = get_project_tree()

formatted_contributors = [{'name': value['name'], 'email': value['email']} for value in contributors]

folders = [{'folder_name': value['name'], 'folder_path': value['path'], 'folder_mode': value['mode']} for value in project_tree if value['type'] == 'tree']
files = [{'file_name': value['name'], 'file_path': value['path'], 'file_mode': value['mode']} for value in project_tree if value['type'] == 'blob']

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


with open("Python/gitlab_result.json", "w") as f:
    json.dump(my_dict, f, indent=2)
