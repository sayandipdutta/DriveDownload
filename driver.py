# import global_config
from downoptions import *

for instr in instruction_set:
    # breakpoint()
    if instr.special:
        downloader = globals()[instr.method](instr.args, instr.target, instr.special)
    else:
        downloader = globals()[instr.method](instr.args, instr.target)

    downloader.download()
