import requests
 
gitlab_api_url = 'https://gitlab.com/api/v4'
gitlab_project_id = '1794617'
gitlab_access_token = 'glpat-uwSi-st4dNJVZ1L4PfcZ'
headers = {
        'PRIVATE-TOKEN': gitlab_access_token,
    }
 
url = f'{gitlab_api_url}/projects/{gitlab_project_id}/repository/tree'
print(url)
 
response = requests.get(url, headers=headers)
 
if response.status_code == 200:
    print(response.json())
else:
    print(f'Error: {response.status_code}')
    print(response.text)