from global_config import *
import shutil
from googleapiclient.errors import HttpError
from pathlib import Path
from os.path import join as pjoin
import pandas as pd
from utils import *
from typing import Union

# __all__ = ['FilmDB', 'FileDownload', 'FolderDownload', 'Custom']

class FilmDB:
    """
    Searches for film via different queries.
    """
    __slots__ = {
        '_filename': 'full path of database file',
        '_db': 'Dataframe of film information',
        'columns': 'Column names of the database',
        '_file_without_path': 'database filename without path'
    }

    def __init__(self, filename: str=DATABASE):
        self._filename = (
            HOME / DATA_DIR / filename
            if not Path(filename).is_file()
            else Path(filename)
        )
        self._db = pd.read_excel(self._filename)
        self.columns = [col for col in self._db.columns
                        if not col.startswith('Unnamed')]
        self._db = self._db[self.columns]
        self._db = self._db[self._db[self.columns[0]].notnull()]
        self._file_without_path = self._filename.name

    def search(
        self,
        term: str, 
        director: bool=False, 
        case: bool=False
    ) -> pd.Series:
       """
       Search films in database.
       If director==True, get i-th column where i==1
       if director==False,get i-th column where i==0
       -----------------------------------------------
       args      -> term       => str
                 -> director   => bool (default: False)
                 -> case       => bool (default: False)

       returns   -> entries that have matching term => pd.Series
       """

       s = self._db[self.columns[director]]
       if case == False:
            s = s.str.casefold()
            term = term.casefold()
       return self._db[s.str.contains(term)]


    @measure_time
    def get_films(self, by: str = 'director', **kwargs: Any) -> list[str]:
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

        if not by == 'director':
            raise NotImplementedError(
                    "Only 'director' is supported argument for keyword 'by'"
                )

        director = kwargs.get('name').lower()
        if director:
            self._db = self._db[self._db['Director'].notna()]
            films = (
                self._db[self._db['Director']
                .str.contains(director)]
                .sort_values(by='Year')['Movie Name (Year)']
                .tolist()
            )

            return films
        else:
            raise NotImplementedError

    @classmethod
    def refresh(cls, filename: list[str]=['Film List'], local: bool=False) -> Union["FileDownload", None]:
        """ Refresh the FilmDB.
        If called without an argument: refreshes the same file from git.
        If filename is provided. Checks whether it is local,
        else downloads that filename."""

        if local:
            filename, = filename
            local_candidates = [
                filename,
                ABSPATH / filename,
                ABSPATH / DATA_DIR / filename,
                ABSPATH / CONFIG_DIR / filename,
                ABSPATH / BASE_TARGET / filename
            ]
            match = next(candidate for candidate in local_candidates
                       if candidate.is_file())

            if match:
               shutil.copyfile(match, ABSPATH / DATA_DIR / filename)

        else:
            FileDownload(filename, BASE_TARGET).download()

    def __str__(self):
        return f"databse_name: {self._file_without_path}"

    def __repr__(self):
        return f"<FilmDB object of file {self._file_without_path}>"


class BaseDownloader:
    def __init__(self):
        self.files = None
        self.tot_files = None
        self.target = None
        self.special = None
        self.tot_folders = None
 #       self.last_updated_file = None

    @staticmethod
    def get_file_id(file_name: str, parent=None) -> str:
        """
        Add docstrings
        """
        query = ["mimeType != 'application/vnd.google-apps.folder'",
                  f"name = '{file_name}'"]
        query = ' and '.join(query)
        file_name = file_name.replace("'", "\\'")
        query = f"name = '{file_name}'"
        files = DRIVE.files().list(q=query,
                                    corpora='user'
                                    ).execute().get('files', [])
        if not files:
            raise FileNotFoundError(2, "No such file or directory", file_name)
        elif len(files) > 1:
            if not parent:
                return files[0]['id']
                raise ValueError("More than one value found", files)
            else:
                file_name = parent.replace("'", "\\'")
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

    def download(self):
        """
        Base download method.
        """
        query = [
            "mimeType != 'application/vnd.google-apps.folder'",
            f"name = '{file_name}'"
            ]
        query = ' and '.join(query)
        file_name = file_name.replace("'", "\\'").replace("\\", r'\\')
        query = f"name contains '{file_name}'"
        files = DRIVE.files().list(q=query, 
                                    corpora='user'
                                    ).execute().get('files', [])

        if not files:
            raise FileNotFoundError(2, "No such file or directory", file_name)
        elif len(files) > 1:
            if not parent:
                return files[0]['id']
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
    def write_success(fullpath: str, filename: str) -> None:
        '''
        Write filename at fullpath in 'success.txt'.
        '''
        with open(pjoin(fullpath, 'success.txt'), 'a+') as f:
            f.write(filename + '\n')


    @staticmethod
    def check_success(path: str, file_name: str) -> bool:
        '''
        Check if success.txt exists in path, and file_name 
        exists in success.txt, if both True return True, 
        else False
        ----------------------------------------------------
        args      -> path      => str,
                  -> file_name => str

        returns   -> success   => bool

        ----------------------------------------------------
        '''
        if 'success.txt' in os.listdir(path):
            return file_name in open(
                    pjoin(path, 'success.txt')
                    ).read()
        return False


    @staticmethod
    def download_file(file_id: str, file_name: str) -> bool:
        '''
        Download files given file_id and save it in filename
        ------------------------------------------------------
        args      -> file_id   => str,
                  -> file_name => str
        
        returns   -> complete  => bool

        ------------------------------------------------------
        complete==True if file download successful else False
        '''

        total_size_bytes = total_size = int(
            DRIVE.files()
            .get(fileId=file_id, fields='size')
            .execute()
            .get('size')
        )
        unit = 'bytes'

        if total_size // 2**30:
            total_size /= 2**30
            unit = 'GB'
        elif total_size // 2**20:
            total_size /= 2**20
            unit = 'MB'
        elif total_size // 2**10:
            total_size /= 2**10
            unit = 'KB'

        print(f'Size: {total_size:.2f} {unit}')
        request = DRIVE.files().get_media(fileId=file_id)
        fh = FileIO(file_name, 'wb')
        downloader = MediaIoBaseDownload(fh,
                                        request,
                                        chunksize=150 * 1024 * 1024)
        complete = False
        start_time = time.time()
        plausible_error = (
            "Only files with binary content can be downloaded. "
            "Use Export with Google Docs files."
            )

        try:
            if total_size_bytes > 10 * 2**20: print('Starting ...')
            while complete is False:
                status, complete = downloader.next_chunk()
                print('\r', end= download_info(status, start_time))
        except HttpError as e:
            if plausible_error in str(e):
                print(
                    'Probabale Doc/Sheet file. '
                    'switching to Doc mode.'
                )
                mimeType=('application/'
                            'vnd.openxmlformats-officedocument'
                            '.spreadsheetml.sheet'
                            )
                request = DRIVE.files().export_media(
                    fileId=file_id, 
                    mimeType=mimeType,
                    )
                with open(file_name.rsplit('.')[0] + '.xlsx', 'wb') as f:
                    f.write(request.execute())
                complete = True

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
        if folder_id:
            files = DRIVE.files().list(
                                        q=f"'{folder_id}' in parents",
                                        corpora='user'
                                    ).execute().get('files', [])
            if not files:
                raise ValueError(f"Empty folder: {folder_name}")
            return files
        else:
            folder_name = folder_name.replace("'", "\\'")
            query = (f"name contains '{folder_name}' "
                      "and mimeType contains 'folder'")
            print(query)
            folders = DRIVE.files().list(
                                            q=query, 
                                            corpora='user'
                                    ).execute().get('files', [])
            
            if not folders:
                raise ValueError("Empty Folder")
            if len(folders) > 1:
                print(*enumerate(folders, start=1))
                ix = int(input("Multiple items found, choose one: "), sep='\n')
            else: ix = 0
            folder_id = folders[ix]['id']
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
        self.tot_files = len(self.files)
        print(f"Content:", *self.files, sep='\n')
        for count, file in enumerate(self.files, start=1):
            file_id = self.get_file_id(file, self.parent)
            filename = pjoin(self.target, file)

            if self.indivdual_folders:
                fullpath = Path(filename).with_suffix('')

                if not os.path.isdir(fullpath):
                    os.mkdir(fullpath)
                    filename = pjoin(fullpath, file)
            else:
                fullpath = self.target
           
            if self.check_success(fullpath, file):
                print(f"{file} already present")
                continue

            print(f"Downloading {count}/{self.tot_files}\n"
                    f"Saving {file} in {fullpath} progress:")
            complete = self.download_file(file_id, filename)
            if complete:
                self.write_success(fullpath, file)

            print("File saved.")


class FolderDownload(BaseDownloader):
    def __init__(self, folders, target, nested=False):
        super().__init__()
        self.folders = folders
        self.tot_folders = len(folders)
        self.target = target
        if not os.path.isdir(self.target):os.mkdir(self.target)
        self.nested = nested

    #TODO: implement recursion
    def download(self):
        folderid = None
        if self.nested:
            folder_contents = self.get_folder_content(folder_name=self.folders[0])
            folderid = self.get_file_id(self.folders[0])
            self.folders = [folder['name'] for folder in folder_contents]
        self.tot_folders = len(self.folders)
        print(f"Content:", *self.folders, sep='\n')
        for count, folder in enumerate(self.folders, start=1):
            try:
                folder_id = self.get_file_id(folder, folderid)

            except (HttpError, ValueError, FileNotFoundError, Exception) as e:
                print(e)
                continue
            folder_name = pjoin(self.target, folder)
            folder_contents = self.get_folder_content(folder, folder_id)
            fl_downloader = FileDownload(([file['name'] for file 
                                            in folder_contents], 
                                          folder_id), 
                                          folder_name, 
                                          False
                                        )

            print(f"Downloading {count}/{self.tot_folders}\n"
                  f"Saving {folder} in {self.target} progress:")
            try:
                fl_downloader.download()
            except ValueError as e:
                print(e)
                continue
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
                self.target = pjoin(self.target, query)
                if not os.path.isdir(self.target):os.mkdir(self.target)
                print(f"Downloading films of {query}")
                fl_downloader = FolderDownload(folders, self.target)
                try:
                    fl_downloader.download()
                except ValueError as e:
                    print(e)
                    continue



    


    
