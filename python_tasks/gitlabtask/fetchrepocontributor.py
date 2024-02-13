import requests

 
def get_contributors(api_url, project_id, access_token):
    headers = {
        'PRIVATE-TOKEN': access_token,
    }
 
    url = f'{api_url}/projects/{project_id}/repository/contributors'

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f'Error: {response.status_code}')
        print(response.text)
        return None