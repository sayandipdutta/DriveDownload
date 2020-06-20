from global_config import *
from googleapiclient.errors import HttpError
from pathlib import Path
import pandas as pd
from utils import *


class FilmDB:
    """
    Searches for film via different queries.
    """
    __slots__ = {'_filename': 'full path of database file',
                 '_db': 'Dataframe of film information',
                 '_file_without_path': 'database filename without path'
    }

    def __init__(self, filename: str):
        self._filename = os.path.join(BASE_TARGET, filename)
        self._db = pd.read_csv(self._filename)
        self._file_without_path = self._filename.rsplit('/')[-1]

    @measure_time
    def get_films(self, by: str = 'director', **kwargs):
        """ 
        Get films by director.
        -----------------------------------------------
        args    -> by    => str (default: 'director'),
                -> name  => str (name of the director)

        returns -> films => list

        -----------------------------------------------
        provided by=='director' and name==<some_name>,
        return list of films by that director from the db.
        """
        if by != 'director':
            raise NotImplementedError

        director = kwargs.get('name', None)
        if director:
            self._db = self._db[self._db['Director'].notna()]
            films = self._db[self._db['Director'
                            ].str.contains(director
                            )].sort_values(by='Year'
                            ).filter(like='Movie Name'
                            ).values.flatten().tolist()

            return films
        else:
            raise NotImplementedError

    def __str__(self):
        return f"databse_name: {self._str}"

    def __repr__(self):
        return f"<FilmDB object of file {self._str}>"


class BaseDownloader:
    def __init__(self):
        self.files = None
        self.tot_files = None
        self.target = None
        self.special = None
        self.tot_folders = None

    def download(self):
        pass

    @staticmethod
    def get_file_id(file_name: str, parent=None) -> str:
        """
        Add docstrings
        """

        query = ["mimeType != 'application/vnd.google-apps.folder'",
                 f"name = '{file_name}'"]
        query = ' and '.join(query)
        query = f"name = '{file_name}'"
        
        files = DRIVE.files().list(q=query, 
                                    corpora='user'
                                    ).execute().get('files', [])

        if not files:
            raise FileNotFoundError(2, "No such file or directory", file_name)
        elif len(files) > 1:
            if not parent:
                raise ValueError("More than one value found", files)
            else:
                query += f' and "{parent}" in parents'
                files = DRIVE.files().list(q=query, 
                                            corpora='user'
                                            ).execute().get('files', [])
                if len(files) > 1:
                    raise ValueError("More than one value found", files)
                else:
                    return files[0]['id']
        else:
            return files[0]['id']

    @staticmethod
    def download_file(file_id: str, file_name: str) -> bool:
        '''
        Download files given file_id and save it in filename
        ------------------------------------------------------
        args      -> file_id   => str,
                  -> file_name => str
        
        returns   -> success   => bool

        ------------------------------------------------------
        complete==True if file download successful else False
        '''

        request = DRIVE.files().get_media(fileId=file_id)
        fh = FileIO(file_name, 'wb')
        downloader = MediaIoBaseDownload(fh, request, chunksize=150 * 1024 * 1024)
        complete = False
        start_time = time.time()

        while complete is False:
            status, complete = downloader.next_chunk()
            print('\r', end= download_info(status, start_time))
        print()
        return complete

    @staticmethod
    @measure_time
    def get_folder_content(folder_name: str = None, 
                            folder_id: str = None) -> list:
        '''
        Callable to get contents of a folder
        ------------------------------------------------------
        args     -> folder_name => str / None,
                -> folder_id   => str / None

        returns  -> files       => list

        ------------------------------------------------------
        Given a folder name generates id, and recursively calls 
        itself with id, and then generates list of files/folders
        in the given folder_name.
        '''
        # breakpoint()
        if folder_id:
            files = DRIVE.files().list(
                                        q=f"'{folder_id}' in parents",
                                        corpora='user'
                                    ).execute().get('files', [])
            if not files:
                raise ValueError("Empty folder")
            return files
        else:
            query = f"name = '{folder_name}' and mimeType contains 'folder'"
            folders = DRIVE.files().list(
                                            q=query, 
                                            corpora='user'
                                    ).execute().get('files', [])
            
            if not folders:
                raise ValueError("Empty Folder")
            folder_id = folders[0]['id']
            return BaseDownloader.get_folder_content(folder_name=None, folder_id=folder_id)

    

class FileDownload(BaseDownloader):
    def __init__(self, files, target, indivdual_folders=False):
        super().__init__()

        if type(files) == tuple:
            self.files, self.parent = files
        else:
            self.files, self.parent = files, None
        self.tot_files = len(files)
        self.target = target
        self.indivdual_folders = indivdual_folders
        self.file_type = type(files[0])

    def download(self):
        if not os.path.isdir(self.target):os.mkdir(self.target)
        for count, file in enumerate(self.files, start=1):
            file_id = self.get_file_id(file, self.parent)
            filename = os.path.join(self.target, file)

            if self.indivdual_folders:
                fullpath = Path(filename).with_suffix('')

                if not os.path.isdir(fullpath):
                    os.mkdir(fullpath)
                    filename = os.path.join(fullpath, file)
            else:
                fullpath = self.target
            
            if 'success.txt' in os.listdir(fullpath) and \
                file in open(os.path.join(fullpath, 'success.txt')).read():
                print(f"{file} already present")
                continue
            print(f"Downloading {count}/{self.tot_files}\nSaving {file} in {fullpath} progress:")
            complete = self.download_file(file_id, filename)
            if complete:
                with open(os.path.join(fullpath, 'success.txt'), 'a+') as f:
                    f.write(file + '\n')

            print("File saved.")

class FolderDownload(BaseDownloader):
    def __init__(self, folders, target, nested=False):
        super().__init__()
        self.folders = folders
        self.tot_folders = len(folders)
        self.target = target
        self.nested = nested

    #TODO: implement recursion
    def download(self):
        folderid = None
        if self.nested:
            folder_contents = self.get_folder_content(folder_name=self.folders[0])
            folderid = self.get_file_id(self.folders[0])
            self.folders = [folder['name'] for folder in folder_contents]
        self.tot_folders = len(folder)
        for count, folder in enumerate(self.folders, start=1):
            try:
                folder_id = self.get_file_id(folder, folderid)

            except (HttpError, ValueError, FileNotFoundError, Exception) as e:
                print(e)
                continue
            folder_name = os.path.join(self.target, folder)
            folder_contents = self.get_folder_content(folder, folder_id)
            fl_downloader = FileDownload(([file['name'] for file in folder_contents], folder_id), folder_name, False)

            print(f"Downloading {count}/{self.tot_folders}\nSaving {folder} in {self.target} progress:")
            fl_downloader.download()
            print("File saved.")

class Custom(BaseDownloader):
    def __init__(self, search_terms, target, query_type):
        super().__init__()
        self.target = target
        self.search = search_terms
        self.query_type = query_type
        self.filmdb = FilmDB(DATABASE)

    def download(self):
        if self.query_type.capitalize() == 'Director':
            for query in self.search:
                folders = self.filmdb.get_films(by='director', name=query)
                self.target = os.path.join(self.target, query)
                if not os.path.isdir(self.target):os.mkdir(self.target)
                print(f"Downloading films of {query}")
                fl_downloader = FolderDownload(folders, self.target)
                fl_downloader.download()



    


    
