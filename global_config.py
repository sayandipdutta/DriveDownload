import os
import sys

# from utils import *
from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from io import FileIO
from httplib2 import Http
from oauth2client import file, client, tools
from ast import literal_eval
from collections import namedtuple

CONFIG = sys.argv[1]

instruction_set = []

def __instructions(class_type, args, target, special):
    instruction = namedtuple('instruciton', 'method args target special')
    if target == '.':
        target = BASE_TARGET
    elif not os.path.isdir(target):
        target = os.path.join(BASE_TARGET, target)
    elif os.path.isdir(target):
        target = target
    else:
        raise FileNotFoundError(2, 'No such file or directory', target)
    
    args = args.split('^')
    if class_type == 'FileDownload':
        special = literal_eval(special)

    return instruction(class_type, args, target, special)

with open(CONFIG) as config:
    for line in config.read().splitlines():
        if not line or line.startswith('#'): continue

        if line.startswith('SCOPES'):
            SCOPES = line.split('=')[-1]

        if line.startswith('HOME'):
            HOME = line.split('=')[-1]

        if line.startswith('BASE_TARGET'):
            BASE_TARGET = line.split('=')[-1]

        if line.startswith('CRED_FILES'):
            storage_file, cred_file = line.split('=')[-1].split(',')
            store = file.Storage(os.path.join(HOME, storage_file))
            creds = store.get()

            if not creds or creds.invalid:
                flow = client.flow_from_clientsecrets('client_secrets.json', SCOPES)
                creds = tools.run_flow(flow, store)

            DRIVE = discovery.build('drive', 'v3', http=creds.authorize(Http()))

            globals().update({'DRIVE': DRIVE})

        if line.startswith('OVERRIDE'):
            OVERRIDE_BASE_TARGET = literal_eval(line.split('=')[-1])

        if line.startswith('TARGET'):
            TARGET = line.split('=')[-1]

        if line.startswith('DATABASE'):
            DATABASE = line.split('=')[-1]

        if line.startswith('DOWNLOAD'):
            args = line.split('=')[-1].split('|')
            download_info = __instructions(*args)
            instruction_set.append(download_info)






