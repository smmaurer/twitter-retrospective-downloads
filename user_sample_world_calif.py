__author__ = "Sam Maurer"
__date__ = "September 22, 2017"
__license__ = "MIT"

import pandas as pd
import time

# runtime hack to import code from a subfolder
import sys
sys.path.insert(0, 'rest_automator/')

import rest_automator 


users = pd.read_csv('input/user_sample_world_calif.csv').user_id.tolist()

a = rest_automator.Automator(
        user_ids = users,
        ts_min = time.strptime('Aug 1 2017 0:00:00', '%b %d %Y %H:%M:%S'),
        ts_max = time.strptime('Sep 1 2017 0:00:00', '%b %d %Y %H:%M:%S'),
        geo_only = False,
		fname_base = 'user-sample-world-calif-')

a.download()