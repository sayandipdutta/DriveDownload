import json
import pathlib
import os
from os.path import join as pjoin
from typing import NamedTuple, Any
from utils import PathLike, chdir

from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from io import FileIO
from httplib2 import Http
from oauth2client import file, client, tools
from ast import literal_eval
from collections import namedtuple
import argparse

parser = argparse.ArgumentParser(
    description="Get config and settings files."
)
parser.add_argument(
    "-c", "--config", 
	help="which config to load from", 
    type=str, 
    default='instructions.ini'
)
parser.add_argument(
    "-s", "--setting", 
	help="name of setting_file", 
    type=str, 
    default='settings.json'
)
args = parser.parse_args()


instruction_set = []
ABSPATH = pathlib.Path(__file__).parent.absolute()
CONFIG_DIR = 'config'
CONFIG = ABSPATH / CONFIG_DIR / args.config
SETTING = ABSPATH / CONFIG_DIR / args.setting

class Instruction(NamedTuple):
    method: str
    args: list[str]
    target: pathlib.Path
    special: Any


def __instructions(
    class_type: str, 
    args: str, 
    target: PathLike, 
    special: str) -> Instruction:
    """Create instruction sets.

    Args:
        class_type (str): Name of the class to assign.
        args (str): Arguments to the class
        target (PathLike): Path to save.
        special (str): Any special treatment.

    Raises:
        FileNotFoundError: If Target is invalid.

    Returns:
        namedtuple[str, list[str], pathlib.Path, Any]: Args for class.
    """

    
    if target == '.':
        target = BASE_TARGET
    elif not os.path.isdir(target):
        target = pathlib.Path(BASE_TARGET) / target
    elif pathlib.Path(target).is_dir():
        target = target
    else:
        raise FileNotFoundError(2, 'No such file or directory', target)
    
    args = args.split('^')
    if class_type != 'Custom':
        special = literal_eval(special)

    return Instruction(class_type, args, target, special)

with chdir(ABSPATH):
    
    with \
        open(SETTING, 'r+') as setting_file, \
        open(CONFIG) as configs \
    :
        settings = json.load(setting_file)
        # globals.update(settings)
        if not (ABSPATH / CONFIG_DIR / 'storage.json').is_file():
        # if True:
            HOME = ABSPATH
            settings.update(HOME=ABSPATH)
            setting_file.truncate(0)
            json.dump(settings, setting_file, indent='\t')
        locals().update(settings)
        storage_file, cred_file = map(
            lambda x: ABSPATH / CONFIG_DIR / CRED_FILES.get(x), 
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

