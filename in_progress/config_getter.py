import json,sys
print(sys.path[0])# = "c:\Users\91790\Documents\project_repo"
class GetConfig:
    def __init__(self):
        pass
    path = "config/path.json"


    def get_path_configs(path = path):
        with open(path) as json_data_file:
            config = json.load(json_data_file)
        return config


