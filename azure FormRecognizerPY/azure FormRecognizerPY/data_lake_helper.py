from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings


def initialize_storage_account(storage_account_name, storage_account_key):
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)



def list_directory_contents(directory_Name):
    try:

        paths = file_system_client.get_paths(path=directory_Name)

        return paths

    except Exception as e:
     print(e)


def download_file(container_Name,directory_Name,file_Name):
    try:

        file_system_client = service_client.get_file_system_client(file_system=container_Name)
        directory_client = file_system_client.get_directory_client(directory_Name)
        file_client = directory_client.get_file_client(file_Name)

        download = file_client.download_file()

        return download

    except Exception as e:
     print(e)


def upload_file_sink(container_Name_Sink,directory_Name_Sink,file_Name_Sink,data):
    try:

        file_system_client_Sink = service_client.get_file_system_client(file_system=container_Name_Sink)
        directory_client_Sink = file_system_client_Sink.get_directory_client(directory_Name_Sink)
        file_client_Sink = directory_client_Sink.get_file_client(file_Name_Sink)

        file_client_Sink.upload_data(data, overwrite=True)

        return "Fileupload " + file_Name_Sink +  " successfull"

    except Exception as e:
     print(e)
