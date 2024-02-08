import requests
import json
 
class GitLabAPI:
    def __init__(self):
        self.project_id = None
        self.project_name = None
        self.folders_count = 0
        self.files_count = 0
        self.contributors = []
        self.files = []
        self.folders = []
 
    def get_contributors(self, api_url, project_id, access_token):
        contributor_dict = {}
        headers = {
            'PRIVATE-TOKEN': access_token,
        }
        url = f'{api_url}/projects/{project_id}/repository/contributors'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for i in data:
                contributor_dict['name'] = i['name']
                contributor_dict['email'] = i['email']
                self.contributors.append(contributor_dict.copy())  # Fix: Create a new dictionary for each contributor
            return self.contributors
        else:
            print(f'Error: {response.status_code}')
            print(response.text)
            return None
 
    def get_meta_data(self, project_tree):
        folder = {}
        files = {}
        if project_tree:
            for item in project_tree:
                if item['type'] == 'tree':
                    self.folders_count += 1
                    folder['folder_name'] = item['name']
                    folder['folder_path'] = item['path']
                    folder['folder_mode'] = item['mode']
                    self.folders.append(folder.copy())  # Fix: Create a new dictionary for each folder
                if item['type'] == 'blob':
                    self.files_count += 1
                    files['files_name'] = item['name']
                    files['files_path'] = item['path']
                    files['files_mode'] = item['mode']
                    self.files.append(files.copy())  # Fix: Create a new dictionary for each file
 
    def get_project_tree(self, api_url, project_id, access_token):
        headers = {
            'PRIVATE-TOKEN': access_token,
        }
        params = {
            'recursive': 'true',
            'per_page': '100',
        }
        url = f'{api_url}/projects/{project_id}/repository/tree/'
        all_data = []
 
        while url:
            response = requests.get(url, headers=headers, params=params)
 
            if response.status_code == 200:
                data = response.json()
                all_data.extend(data)
                 #print('response.headers',response.headers)
                # Check if there is another page
                link_header = response.headers.get('Link', '')
                #print("link_header", link_header)
                next_page_url = self.extract_next_page_url(link_header)
 
                if next_page_url:
                    url = next_page_url
                else:
                    # If no next page, exit the loop
                    url = None
            else:
                print(f'Error: {response.status_code}')
                print(response.text)
                return None
 
        self.get_meta_data(all_data)
        return all_data
 
 
    def get_project_details(self, api_url, project_id, access_token):
        headers = {
            'PRIVATE-TOKEN': access_token,
        }
        url = f'{api_url}/projects/{project_id}'
        response = requests.get(url, headers=headers)
 
        if response.status_code == 200:
            data = response.json()
            self.project_id = data['id']
            self.project_name = data['name']
            return response.json()
        else:
            print(f'Error: {response.status_code}')
            print(response.text)
            return None
 
    def generate_data(self):
        print('folders_count',self.folders_count)
        print('file',self.files_count)
 
        return {
            "id": self.project_id,
            "name": self.project_name,
            "contributors": self.contributors,
            "contents": {
                "meta": {
                    "folders_count": self.folders_count,
                    "files_count": self.files_count
                },
                "folders": self.folders,
                "files": self.files,
            }
        }
    
    @staticmethod
    def extract_next_page_url(link_header):
        # Extract the next page URL from the 'Link' header
        links = link_header.split(', ')
        for link in links:
            if 'rel="next"' in link:
                return link.split(';')[0][1:-1]
        return None
 
 
c1 = GitLabAPI()
gitlab_api_url = 'https://gitlab.com/api/v4'
gitlab_project_id = '1794617'
gitlab_access_token = 'glpat-uwSi-st4dNJVZ1L4PfcZ'
c1.get_contributors(gitlab_api_url, gitlab_project_id, gitlab_access_token)
c1.get_project_details(gitlab_api_url, gitlab_project_id, gitlab_access_token)
c1.get_project_tree(gitlab_api_url, gitlab_project_id, gitlab_access_token)
data = c1.generate_data()
print(data)


with open('Python/gitlab_data.json', 'w') as f:
    json.dump(data, f, indent=4)
    print('Data written to gitlab_data.json')