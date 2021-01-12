import json

from AbstractJSONDataLakeConnector import JSONDataLakeConnection
from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient


class AzureDataLakeConnector(JSONDataLakeConnection):

    def __init__(self, storage_account_name, storage_account_key, container_name):
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key

        self._connect()
        self._create_file_system(container_name)

    def _connect(self):
        url = f"https://{self.storage_account_name}.dfs.core.windows.net"
        self.service_client = DataLakeServiceClient(account_url=url, credential=self.storage_account_key)

    def _create_file_system(self, container_name):
        try:
            self.file_system_client = self.service_client.create_file_system(file_system=container_name)
        except ResourceExistsError:
            self.file_system_client = self.service_client.get_file_system_client(file_system=container_name)

        self._cwd = self.file_system_client.get_directory_client("/")

    def mkdir(self, path: str):
        self.file_system_client.create_directory(path)

    def rmdir(self, path: str):
        self._cwd.delete_sub_directory(path)

    def store(self, serialized_json_content: str, filename: str, overwrite=False):
        file_client = self._cwd.create_file(filename)
        file_client.upload_data(serialized_json_content, overwrite=overwrite)

    def retrieve(self, filename: str) -> dict:
        file_client = self._cwd.get_file_client(filename)
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        return json.loads(downloaded_bytes)

    def rm(self, filename: str):
        self._cwd.get_file_client(filename).delete_file()

    def ls(self, path: str) -> [str]:
        return [p.name for p in self.file_system_client.get_paths(path=path)]
