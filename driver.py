# import global_config
import subprocess
import os

if not os.path.isfile('storage.json'):
    cmd = ['python authorize.py --noauth_local_webserver']
    subprocess.run(cmd, check=True, shell=True)

from downoptions import *

for instr in instruction_set:
    if instr.special:
        downloader = globals()[instr.method](instr.args, instr.target, instr.special)
    else:
        downloader = globals()[instr.method](instr.args, instr.target)

    downloader.download()
