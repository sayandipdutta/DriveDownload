import global_config
from downoptions import *

for instr in global_config.instruction_set:
    if instr.special:
        downloader = globals()[instr.method](instr.args, instr.target, instr.special)
    else:
        downloader = globals()[instr.method](instr.args, instr.target)

    downloader.download()
