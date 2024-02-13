import requests
 
def get_project_tree(api_url, project_id, access_token):
    headers = {
        'PRIVATE-TOKEN': access_token,
    }
 
    params ={
        'pagination':'keyset',
        'per_page':100,
 
    }
 
    url = f'{api_url}/projects/{project_id}/repository/tree/'
 
    response = requests.get(url, headers=headers, params=params)
 
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Error: {response.status_code}')
        print(response.text)
        return None
 
if __name__ == '__main__':
    gitlab_api_url = 'https://gitlab.com/api/v4'
    gitlab_project_id = '1794617'
    gitlab_access_token = 'glpat-v6Jx7TiMn-TyxKsGx7zU'
 
    project_tree = get_project_tree(gitlab_api_url, gitlab_project_id, gitlab_access_token)
    print(project_tree)
    folder_count=0
    file_count=0
    if project_tree:
        for item in project_tree:
            if(item['type']=='tree'):
                folder_count +=1
            else:
                file_count +=1
        
        print('folder_count', folder_count)
        print('file_count', file_count)