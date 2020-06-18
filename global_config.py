import os
import sys

from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from io import FileIO
from httplib2 import Http
from oauth2client import file, client, tools
from ast import literal_eval
from collections import namedtuple

CONFIG = sys.argv[1]

SCOPES = 'https://www.googleapis.com/auth/drive'
HOME = '/home/sayandip199309/projects_master/DriveDownload'
BASE_TARGET = '/mnt/d/Movies'
OVERRIDE_BASE_TARGET = False

instruction_set = []

storage_file = 'storage.json'
cred_file = 'client_secrets.json'
store = file.Storage(os.path.join(HOME, storage_file))
creds = store.get()

if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets('client_secrets.json', SCOPES)
    creds = tools.run_flow(flow, store)

DRIVE = discovery.build('drive', 'v3', http=creds.authorize(Http()))

globals().update({'DRIVE': DRIVE})

with open(CONFIG) as config:
    for line in config.read().splitlines():
        if not line or line.startswith('#'): continue

        if line.startswith('OVERRIDE'):
            OVERRIDE_BASE_TARGET = literal_eval(line.split('=')[-1])

        if line.startswith('TARGET'):
            TARGET = line.split('=')[-1]

        if line.startswith('DATABASE'):
            DATABASE = line.split('=')[-1]

        if line.startswith('DOWNLOAD'):
            args = line.split('=')[-1].split('|')
            downlod_info = __instructions(*args)
            instruction_set.append(download_info)


def __instrucitons(class_type, args, target, special):
    instruction = namedtuple('instruciton', 'method args type special')
    if target == '.':
        target = BASE_TARGET
    elif not os.path.isdir(target):
        target = os.path.join(BASE_TARGET, target)
    elif os.path.isdir(target):
        target = target
    else:
        raise FileNotFoundError(2, 'No such file or directory', target)
    
    args = args.split('^')

    return instruction(class_type, args, target, special)




