import requests
import json

class GitlabAPI:
    def __init__(self, gitlab_token):
        self.api_url = 'https://gitlab.com/api/v4'
        self.gitlab_token = gitlab_token
        self.headers = {'Private-Token': self.gitlab_token}

    def get_project_info(self, namespace, project):
        try:
            project_endpoint = f'{self.api_url}/projects/{namespace}%2F{project}'
            param = {'recursive': True, 'per_page': 100}
            response = requests.get(project_endpoint, headers=self.headers, params=param)
            response.raise_for_status()  # Raise an exception for non-200 status codes
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error getting project info: {e}")
            return None

    def get_repo_contents(self, namespace, project):
        try:
            repo_contents_endpoint = f'{self.api_url}/projects/{namespace}%2F{project}/repository/tree'
            param = {'recursive': True, 'per_page': 100}
            response = requests.get(repo_contents_endpoint, headers=self.headers, params=param)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error getting repository contents: {e}")
            return None

    def get_contributors(self, namespace, project):
        try:
            contributors_endpoint = f'{self.api_url}/projects/{namespace}%2F{project}/repository/contributors'
            param = {'recursive': True, 'per_page': 100}
            response = requests.get(contributors_endpoint, headers=self.headers, params=param)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error getting contributors: {e}")
            return None

    def generate_project_json(self, namespace, project):
        project_info = self.get_project_info(namespace, project)
        repo_contents = self.get_repo_contents(namespace, project)
        contributor_contents = self.get_contributors(namespace, project)

        if project_info and repo_contents and contributor_contents:
            try:
                dic_api = {}
                dic_api['id'] = project_info['id']
                dic_api['name'] = project_info['name']

                contributors_list = [{'name': contributor['name'], 'email': contributor['email']} for contributor in contributor_contents]
                dic_api['contributors'] = contributors_list

                contents_info = {
                    'meta': {
                        'folders_count': len([item for item in repo_contents if item['type'] == 'tree']),
                        'files_count': len([item for item in repo_contents if item['type'] == 'blob'])
                    },
                    'folders': [],
                    'files': []
                }

                for item in repo_contents:
                    if item['type'] == 'tree':
                        contents_info['folders'].append({
                            'folder_name': item['name'],
                            'folder_path': item['path'],
                            'folder_mode': item['mode']
                        })
                    elif item['type'] == 'blob':
                        contents_info['files'].append({
                            'file_name': item['name'],
                            'file_path': item['path'],
                            'file_mode': item['mode']
                        })

                dic_api['contents_info'] = contents_info
                project_json = json.dumps(dic_api, indent=4)
                return project_json
            except Exception as e:
                print(f"Error generating project JSON: {e}")
                return None
        else:
            return None

# Usage
gitlab_token = 'glpat-Xyti-m98_bxnRYaRYvVf'
namespace = 'gitlab-org'
project = 'gitlab-docs'

gitlab_api = GitlabAPI(gitlab_token)
project_json = gitlab_api.generate_project_json(namespace, project)

if project_json:
    print(project_json)
else:
    print("Failed to retrieve project information.")
