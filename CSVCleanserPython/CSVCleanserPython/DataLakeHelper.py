class DataLakeHelper(object):
    """description of class"""
    # py -m pip install azure-storage-file-datalake
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


def download_metadata_file(container_Name_Meta,directory_Name_Meta,file_Name_Meta):
    try:

        file_system_client_meta = service_client.get_file_system_client(file_system=container_Name_Meta)
        directory_client_meta = file_system_client_meta.get_directory_client(directory_Name_Meta)
        file_client_meta = directory_client_meta.get_file_client(file_Name_Meta)

        download_meta = file_client_meta.download_file()

        return download_meta

    except Exception as e:
     print(e)


def check_directory_exists(container_Name,directory_Name):
    try:

        global file_system_client
        global directory_client
        
        file_system_client = service_client.get_file_system_client(file_system=container_Name)
        directory_client = file_system_client.get_directory_client(directory_Name)

        return directory_client.exists()

    except Exception as e:
     print(e)


def create_directory(container_Name,directory_Name):
    try:

        global file_system_client
        global directory_client
        
        file_system_client = service_client.get_file_system_client(file_system=container_Name_Source)
        directory_client = file_system_client.get_directory_client(directory_Name)

        return directory_client.create_directory()

    except Exception as e:
     print(e)


def list_directory_contents(directory_Name):
    try:

        paths = file_system_client.get_paths(path=directory_Name)

        return paths

    except Exception as e:
     print(e)


def download_file_from_directory(file_Name):
    try:

        file_client = directory_client.get_file_client(file_Name)

        download = file_client.download_file()

        return download

    except Exception as e:
     print(e)


def upload_file_sink(container_Name_Sink,directory_Name_Sink,file_Name_Sink,data,metadataDict={}):
    try:

        global file_client_Sink 

        file_system_client_Sink = service_client.get_file_system_client(file_system=container_Name_Sink)
        directory_client_Sink = file_system_client_Sink.get_directory_client(directory_Name_Sink)
        file_client_Sink = directory_client_Sink.get_file_client(file_Name_Sink)

        content_settings = ContentSettings(content_type = "text/csv", content_encoding="UTF-8")

        file_client_Sink.upload_data(data, overwrite=True, metadata=metadataDict, content_settings=content_settings)

        return "Fileupload " + file_Name_Sink +  " successfull"

    except Exception as e:
     print(e)


