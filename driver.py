# import global_config
import subprocess
import os

if not os.path.isfile('storage.json'):
    cmd = ['python authorize.py --noauth_local_webserver']
    subprocess.run(cmd, check=True, shell=True)

from downoptions import *

for instr in instruction_set:
    if instr.special:
        downloader = globals()[instr.method](
                                                instr.args, 
                                                instr.target, 
                                                instr.special
                                            )
    else:
        downloader = globals()[instr.method](
                                                instr.args, 
                                                instr.target
                                            )
            
    try:
        downloader.download()
    except KeyboardInterrupt:
        last_updated_path = downloader.last_updated_path
        dirname, filename = os.path.split(last_updated_path)
        success = downloader.check_success(path, filename)
        if not success:
            print(f"Last updated file: {filename}"
                    f" at {dirname}, is incomplete"
                    "\n Removing file...")
            os.remove(last_updated_path)
            print("File removed.")
    finally:
        break
