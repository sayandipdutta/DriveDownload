import global_config
from pathlib import Path

@measure_time


class FilmDB:
    """
    Searches for film via different queries.
    """
    __slots__ = {'_filename': 'full path of database file',
                 '_db': 'Dataframe of film information',
                 '_file_without_path': 'database filename without path'
    }

    def __init__(self, filename: str):
        self._filename = filename
        self._db = pd.read_csv(self._filename)
        self._file_without_path = self._filename.rsplit('/')[-1]

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
            films = self._db.query(
                                    'Director == @director'
                            ).sort_values(by='Year'
                            ).filter(like='Movie Name'
                            ).values.flatten().tolist()

            return films, kwargs['name']
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
    def get_file_id(file_name: str) -> str:
        """
        Add docstrings
        """

        query = ["mimeType != 'application/vnd.google-apps.folder'",
                 f"name = {file_name}"]
        query = ' and '.join(query)
        files = DRIVE.files().list(q=query, 
                                corpora='user'
                                ).execute().get('files', [])

        if not files:
            raise FileNotFoundError("File does not exist, please check name")
        elif len(files) > 1:
            raise Exception("More than one value found", files)
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
        downloader = MediaIoBaseDownload(fh, request, chunksize=10 * 1024 * 1024)
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
            return self.get_folder_content(folder_name=None, folder_id=folder_id)

    

class FileDownload(BaseDownloader):
    def __init__(self, files, target, indivdual_folders=False):
        super().__init__(self)
        self.files = files
        self.tot_files = len(files)
        self.target = target
        self.indivdual_folders = indivdual_folders

    def download(self):
        for count, file in enumerate(self.files, start=1):
            file_id = self.get_file_id(file)
            filename = os.path.join(self.target, file)
            if self.indivdual_folders:
                fullpath = Path(fileame).with_suffix('')
                if not os.path.isdir(fullpath):
                    os.mkdir(fullpath)
                    filename = os.path.join(fullpath, file)
            print(f"Downloading {count}/{self.tot_files}\nSaving {file} in {target} progress:")
            self.download_file(file_id, filename)
            print("File saved.")

class FolderDownload(BaseDownloader):
    def __init__(self, files, target):
        super().__init__(self)
        self.folders = folders
        self.tot_folders = len(folders)
        self.target = target

    def download(self):

        for count, folder in enumerate(self.folders, start=1):
            folder_id = self.get_file_id(folder)
            folder_name = os.path.join(self.target, folder)
            folder_contents = self.get_folder_content(folder, folder_id)
            fl_downloader = FileDownload(folder_contents, folder_name, target)

            print(f"Downloading {count}/{self.tot_files}\nSaving {file} in {target} progress:")
            self.download_file(file_id, filename)
            print("File saved.")

    


    
