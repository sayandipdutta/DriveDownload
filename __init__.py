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

import subprocess
import os
from pathlib import Path
parent_cwd = Path.cwd().parent.absolute()

if not (cwd / 'config' / 'storage.json').is_file():
    cmd = 'python authorize.py --noauth_local_webserver'
    subprocess.run(cmd, check=True, shell=True)
