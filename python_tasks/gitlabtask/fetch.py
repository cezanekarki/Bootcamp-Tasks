import requests
 
def get_project_tree(api_url, project_id, access_token):
    headers = {
        'PRIVATE-TOKEN': access_token,
    }
 
    url = f'{api_url}/projects/{project_id}/repository/tree'
 
    response = requests.get(url, headers=headers)
 
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Error: {response.status_code}')
        print(response.text)
        return None
 
if __name__ == '__main__':
    gitlab_api_url = 'https://gitlab.com/api/v4'
    gitlab_project_id = '1794617'
    gitlab_access_token = 'glpat-uwSi-st4dNJVZ1L4PfcZ'
 
    project_tree = get_project_tree(gitlab_api_url, gitlab_project_id, gitlab_access_token)
 
    if project_tree:
        for item in project_tree:
            print(item['name'], item['type'])

