import requests
from urllib.parse import urlparse, quote
import json
from datetime import datetime
 
#https://gitlab.com/api/v4/projects/gitlab-org%2Fgitlab-docs
class GitLabExtractor:
    def __init__(self, token, project_url, base_url):
        self.token = token
        self.project_url = project_url  #https://gitlab.com/gitlab-org/gitlab-docs
        self.base_url = base_url  #"https://gitlab.com/api/v4/projects/"
        self.project_id, self.project_name = self.extract_name_and_id()
        self.contributer_api_url = f"{self.base_url}{self.project_id}/repository/contributors"
        self.folders_and_files_api_url = f"{self.base_url}{self.project_id}/repository/tree"
 
    def extract_name_and_id(self):
        #ParseResult(scheme='https', netloc='gitlab.com', path='/gitlab-org/gitlab-docs', params='', query='', fragment='')
        parsed_url = urlparse(self.project_url).path.strip('/') #gitlab-org/gitlab-docs
        encoded_url = quote(parsed_url, safe=' ')  ##gitlab-org%2Fgitlab-docs
        url = f"{self.base_url}{encoded_url}"
        try:
            response = requests.get(url, headers={'PRIVATE-TOKEN': self.token})
            response.raise_for_status()
            name_and_id = response.json()
            id = name_and_id['id']
            name = name_and_id['name']
            return id, name
        except Exception as e:
            print(f"Failed to retrieve project information: {e}")
            return None, None
 
    def extract_contributers(self):
        try:
            response = requests.get(self.contributer_api_url, headers={'PRIVATE_TOKEN': self.token},
                                    params={'ref': 'main', 'per_page': 100, 'recursive': True})
            response.raise_for_status()
            contributers_info = response.json()
            return [{'name': contributer_info.get('name', ''), 'email': contributer_info.get('email', '')}
                    for contributer_info in contributers_info]
        except Exception as e:
            print(f"Failed to get the contributers info: {e}")
            return []
 
    def extract_folders_and_files(self):
        page = 1
        folders = []
        files = []
        while True:
            response = requests.get(self.folders_and_files_api_url, headers={'PRIVATE-TOKEN': self.token},
                                        params={'ref': 'main', 'per_page': 100, 'page':page, 'recursive': True})
            
            try:
                # response.raise_for_status()
                folders_and_files_info = response.json()
                
                for item in folders_and_files_info:
                    if item.get('type') == 'tree':
                        folders.append({'folder_name': item.get('name', ''), 'folder_path': item.get('path', ''),
                                        'folder_mode': item.get('mode', '')})
                    elif item.get('type') == 'blob':
                        files.append({'files_name': item.get('name', ''), 'files_path': item.get('path', ''),
                                    'files_mode': item.get('mode', '')})
                # return folders, files
                if 'next' in response.links:
                    page += 1
                else:
                    break
                
            except Exception as e:
                print(f"Failed to get the folders and files info: {e}")
                return [], []
        return folders,files
 
    def result_extractor(self):
        extractor_info = {'id': self.project_id,
                          'name': self.project_name,
                          'contributers': [],
                          'contents': {
                              'meta': {
                                  'folders_count': 0,
                                  'files_count': 0
                                },
                                'folders': [],
                                'files': []
                                }
                        }
        try:
            extractor_info['contributers'] = self.extract_contributers()
            folders, files = self.extract_folders_and_files()
            extractor_info['contents']['folders'] = folders
            extractor_info['contents']['meta']['folders_count'] = len(folders)
            extractor_info['contents']['files'] = files
            extractor_info['contents']['meta']['files_count'] = len(files)
        except Exception as e:
            print(f"An error occurred during data extraction: {e}")
        return extractor_info
 
    def dump_to_json(self, data):
        try:
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"repo-query-{timestamp}.json"
            with open(filename, 'w') as json_file:
                json.dump(data, json_file, indent=2)
            print(f"Data dumped to {filename}")
        except Exception as e:
            print(f"Failed to dump data to JSON: {e}")
 
    
        
if __name__ == "__main__":
    token = "glpat--sQAzkCg6Y-hAH_kMFSK"
    project_url = "https://gitlab.com/gitlab-org/gitlab-docs"
    base_url = "https://gitlab.com/api/v4/projects/"
    try:
        gitlab_project = GitLabExtractor(token, project_url, base_url)
        extractor_info = gitlab_project.result_extractor()
        if extractor_info:
            gitlab_project.dump_to_json(extractor_info)
            print("The data is dumped to the JSON format successfully")
        else:
            print("Failed to retrieve project information.")
    except Exception as e:
        print(f"An error occurred: {e}")
 