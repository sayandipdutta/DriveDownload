#!/usr/bin/python
# -*- coding: <UTF-8> -*-
# vim: set fileencoding=<UTF-8> :

#===========================================================================#

"""This program downloads files given a folder name / list of folders
   from Google Drive."""

__author__ = "Sayandip Dutta"
__copyright__ = "Copyright 2020, The Driveloader Project"
__credits__ = ["Sayandip Dutta"]
__lincese__ = 'GPLv3'
__version__ = "0.1"
__maintainer__ = "Sayandip Dutta"
__email__ = "sayandip199309@gmail.com"
__status__ = "Development"

#===========================================================================#
#===========================================================================#

import os
import datetime, time
import pandas as pd
import numpy as np
from functools import wraps
from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from io import FileIO
from httplib2 import Http
from oauth2client import file, client, tools
from typing import Callable



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
        

def measure_time(func: Callable) -> Callable:
    """
    Measure time taken by the Callable.
    -----------------------------------------------------
    args    -> func   => Callable

    returns -> caller => Callable

    -----------------------------------------------------
    Decorator to measure time.
    """
    @wraps(func)
    def caller(*args, **kwargs):
        s_time = time.time()
        result = func(*args, **kwargs)
        t_taken = (time.time() - s_time)
        print(f"{func.__name__} took {t_taken: .2f}s")
        return result
    return caller



def bytes_to_MB(bytes: int) -> float:
    """
    converts bytes to MegaBytes
    ---------------------------
    args   -> bytes => int

    return -> MBytes=> float

    ---------------------------
    """

    return bytes / 2 ** 20

def sec_to_hms(sec: float) -> str:
    """
    converts seconds to human readable hh:mm:ss format
    --------------------------------------------------
    args    -> sec              => float

    returns -> formatted_time   => str

    --------------------------------------------------
    """

    return str(datetime.timedelta(seconds=int(sec)))

def download_info(status, start_time: float) -> str:
    """
    Given status object and a start_time returns current 
    status of the download.
    ---------------------------------------------------
    args    -> status       => status object from MediaIOBasedDownload,
            -> start_time   => float

    returns -> print_string => str

    ---------------------------------------------------
    From status object calculates and returns 
    total_size, total_downloaded, remaining_download, ETA, speed
    """

    total = bytes_to_MB(status.total_size)
    remaining = total * (1 - status.progress())
    curr_time = time.time()
    speed = (total - remaining) / (curr_time - start_time)
    ETA = sec_to_hms(remaining / speed)
    print_string = ' '.join((
        f" Downloaded: {(total - remaining): .2f} MB / {total: .2f} MB",
        f"Remaining: {remaining: .2f} MB",
        f"ETA: {ETA}, speed: {speed: .3f} MBps    "
    ))
    return print_string

@measure_time
def get_by_director(name: str) -> list:
    """
    Callable to get list of films available by a director.

    ------------------------------------------------------
    args     -> name  => str

    returns  -> films => list

    ------------------------------------------------------
    """
    films = df.query(
                    'Director == @name'
                ).filter(like='Movie Name').tolist()
    return films

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

@measure_time
def main():
    """Driver program."""

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
                if 'success.txt' in os.listdir(save_path): continue
                print(f'Downloading {fl["name"]}')
                success = download_file(fl['id'], fl['name'])

                if success:
                    with open('success.txt', 'w') as log:
                        log.write('success\n')
                    continue
                else:
                    raise ValueError("Download interrupted.")


if __name__ == '__main__':
    main()
