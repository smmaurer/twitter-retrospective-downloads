__author__ = "Sam Maurer"
__date__ = "September 18, 2017"
__license__ = "MIT"

import time

# runtime hack to import code from a subfolder
import sys
sys.path.insert(0, 'rest_automator/')

import rest_automator 


users = [25073877, 15446531]

a = rest_automator.Automator(
        user_ids = users,
        ts_min = time.strptime('Jan 1 2016 0:00:00', '%b %d %Y %H:%M:%S'),
        ts_max = time.strptime('Jan 1 2017 0:00:00', '%b %d %Y %H:%M:%S'),
        geo_only = False,
		fname_base = 'all-')

a.download()