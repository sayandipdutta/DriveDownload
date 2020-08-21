import json
import pathlib
import os

from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from io import FileIO
from httplib2 import Http
from oauth2client import file, client, tools
from ast import literal_eval
from collections import namedtuple
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c", "--config", 
	help="which config to load from", 
    type=str, 
    default='instructions.ini'
)
args = parser.parse_args()

instruction_set = []
ABSPATH = pathlib.Path(__file__).parent.absolute().as_posix()
CONFIG = os.path.join(ABSPATH, args.config)

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
    if class_type != 'Custom':
        special = literal_eval(special)

    return instruction(class_type, args, target, special)

with open('settings.json', 'r+') as setting_file, open(CONFIG) as configs:
    settings = json.load(setting_file)
    # globals.update(settings)
    if not os.path.isfile(os.path.join(ABSPATH,'storage.json')):
    # if True:
        HOME = ABSPATH
        settings.update(HOME=ABSPATH)
        setting_file.truncate(0)
        json.dump(settings, setting_file, indent='\t')
    locals().update(settings)
    storage_file, cred_file = map(
        lambda x: os.path.join(ABSPATH, CRED_FILES.get(x)), 
        ['storage_file', 'cred_file']
        )
    store = file.Storage(storage_file)
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(cred_file, SCOPES)
        creds = tools.run_flow(flow, store)
    DRIVE = discovery.build('drive', 'v3', http=creds.authorize(Http()))
    globals().update({'DRIVE': DRIVE})
    
    for line in configs:
        command, args = line.strip().split('=')
        if command == 'DOWNLOAD':
            args = args.split('|')
            download_info = __instructions(*args)
            instruction_set.append(download_info)
        