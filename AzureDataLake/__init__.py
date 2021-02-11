from AbstractDataLake import AbstractDataLake
from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient


class AzureDataLake(AbstractDataLake):

    def __init__(self, storage_account_name, storage_account_key, container_name, app_name):
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self.app_name = app_name

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

        self._ROOT_FOLDER = self.file_system_client.get_directory_client(f"/{self.app_name}")

    def mkdir(self, path: str):
        self.file_system_client.create_directory(f"/{self.app_name}{path}")

    def rmdir(self, path: str, recursive=True):
        if path.startswith("/"):
            path = path[1:]
        self._ROOT_FOLDER.delete_sub_directory(path)

    def store(self, serialized_json_content: str, filename: str, overwrite=False):
        if filename.startswith("/"):
            filename = filename[1:]
        file_client = self._ROOT_FOLDER.create_file(filename)
        file_client.upload_data(serialized_json_content, overwrite=overwrite)

    def retrieve(self, filename: str):
        if filename.startswith("/"):
            filename = filename[1:]

        file_client = self._ROOT_FOLDER.get_file_client(filename)
        download = file_client.download_file()
        return download.readall()

    def rm(self, filename: str):
        if filename.startswith("/"):
            filename = filename[1:]
        self._ROOT_FOLDER.get_file_client(filename).delete_file()

    def ls(self, path: str) -> [str]:
        all_paths = []
        for path in self.file_system_client.get_paths(path=f"/{self.app_name}{path}"):
            all_paths.append(path.name.split(self.app_name)[1])
        return all_paths

    def mvdir(self, dirname: str, new_dirname: str):
        if dirname.startswith("/"):
            dirname = dirname[1:]
        if new_dirname.startswith("/"):
            new_dirname = new_dirname[1:]

        directory_client = self.file_system_client.get_directory_client(f"{self.app_name}/{dirname}")
        directory_client.rename_directory(new_name=f"{directory_client.file_system_name}/{self.app_name}/{new_dirname}")

    def mvfile(self, filepath: str, new_filepath: str):
        if filepath.startswith("/"):
            filepath = filepath[1:]
        if new_filepath.startswith("/"):
            new_filepath = new_filepath[1:]

        fc = self._ROOT_FOLDER.get_file_client(filepath)
        fc.rename_file(f"{self._container_name}/{self.app_name}/{new_filepath}")
