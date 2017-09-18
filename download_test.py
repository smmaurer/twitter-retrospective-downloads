__author__ = "Sam Maurer"
__date__ = "September 17, 2017"
__license__ = "MIT"

import time

# runtime hack to import code from a subfolder
import sys
sys.path.insert(0, 'download_automator/')

import download_automator 


users = [25073877]

d = download_automator.Downloader(
        user_ids = users,
        ts_min = time.strptime('Jul 2 2017 0:00:00', '%b %d %Y %H:%M:%S'),
        ts_max = time.strptime('Jul 3 2017 0:00:00', '%b %d %Y %H:%M:%S'),
		fname_base = 'world-')

d.download()