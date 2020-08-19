import os
import pandas as pd
import numpy as np

from utils import *
from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from io import FileIO
from httplib2 import Http
from oauth2client import file, client, tools




HOME = '/mnt/d/Movies'
SCOPES = 'https://www.googleapis.com/auth/drive'
store = file.Storage('storage.json')
creds = store.get()

if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets('client_secrets.json', SCOPES)
    creds = tools.run_flow(flow, store)

DRIVE = discovery.build('drive', 'v3', http=creds.authorize(Http()))
DATABASE = '/mnt/d/Movies/Film List - list.csv'
MOTHER_DIR = '/home/sayandip199309/projects_master/drive_download'
os.chdir(MOTHER_DIR)

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
            self._db = self._db[self._db['Director'].notna()]
            films = self._db[self._db['Director'
                            ].str.contains(director)
                            ].sort_values(by='Year'
                            ).filter(like='Movie Name'
                            ).values.flatten().tolist()

            return films, kwargs['name']
        else:
            raise NotImplementedError

    def __str__(self):
        return f"databse_name: {self._str}"

    def __repr__(self):
        return f"<FilmDB object of file {self._str}>"
        

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
        return get_folder_content(folder_name=None, folder_id=folder_id)

@measure_time
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


def oversee_download(dir_name, fl):
    if 'success.txt' in os.listdir(dir_name):
        print("File already present.")
        return

    print(f'Downloading {fl["name"]}')
    success = download_file(fl['id'], fl['name'])
    if success:
        with open('success.txt', 'w') as log:
            log.write('success\n')
    return success

@measure_time
def main(type_):
    """Driver program."""

    if type_ == 'file':
        files, dir_name = config.get_args()
        if not os.path.isdir(dir_name): os.mkdir(dir_name); os.chdir(dir_name)
        for fl in files:
            if not 'folder' in fl['mimeType']:
                if 'success.txt' in os.listdir(save_path):
                    if fl['name'] in open('success.txt').read():
                        print(f"{fl['name']} already present.")
                        continue
                print(f'Downloading {fl["name"]}')
                success = download_file(fl['id'], fl['name'])
                if success:
                    with open('success.txt', 'a+') as log:
                        log.write(f'{fl["name"]}\n')
                else:
                    raise ValueError("Download interrupted.")
            oversee_download(dir_name, fl)

    elif type_ == 'folder':
        fdb = FilmDB(DATABASE)
        film_list, director_name = fdb.get_films(by='director', 
                                                name='Abbas Kiarostami')
        curr_home = os.path.join(HOME, director_name)
        if not os.path.isdir(curr_home): os.mkdir(curr_home)

        for folder in film_list:

            save_path = os.path.join(curr_home, folder)
            if not os.path.isdir(save_path): os.mkdir(save_path)
            os.chdir(save_path)

            try:
                files = get_folder_content(folder_name=folder)
            except Exception as e:
                print(e)
                continue
            print(f"Folder: {folder}")

            for fl in files:
                if not 'folder' in fl['mimeType']:
                    oversee_download(save_path, fl)


if __name__ == '__main__':
    main('folder')
