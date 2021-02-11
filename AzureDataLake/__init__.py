from AbstractDataLake import AbstractDataLake
from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient


class AzureDataLake(AbstractDataLake):

    def __init__(self, storage_account_name, storage_account_key, container_name):
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key

        self._connect()
        self._create_file_system(container_name)
        self._container_name = container_name

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

    def rmdir(self, path: str, recursive=True):
        self._cwd.delete_sub_directory(path)

    def store(self, serialized_json_content: str, filename: str, overwrite=False):
        file_client = self._cwd.create_file(filename)
        file_client.upload_data(serialized_json_content, overwrite=overwrite)

    def retrieve(self, filename: str):
        file_client = self._cwd.get_file_client(filename)
        download = file_client.download_file()
        return download.readall()

    def rm(self, filename: str):
        self._cwd.get_file_client(filename).delete_file()

    def ls(self, path: str) -> [str]:
        return [p.name for p in self.file_system_client.get_paths(path=path)]

    def mvdir(self, dirname: str, new_dirname: str):
        directory_client = self.file_system_client.get_directory_client(dirname)
        directory_client.rename_directory(rename_destination=new_dirname)

    def mvfile(self, filepath: str, new_filepath: str):
        fc = self._cwd.get_file_client(filepath)
        fc.rename_file(f"{self._container_name}/{new_filepath}")
