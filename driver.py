# import global_config
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
        last_updated_path = Path(downloader.last_updated_path)
        dirname, filename = last_updated_path.parent, last_updated_path.name
        success = downloader.check_success(dirname, filename)
        if not success:
            print(f"Last updated file: {filename}"
                    f" at {dirname}, is incomplete"
                    "\n Removing file...")
            os.remove(last_updated_path)
            print("File removed.")
    finally:
        break
