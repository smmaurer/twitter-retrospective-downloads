__author__ = "Sam Maurer"
__date__ = "September 18, 2017"
__license__ = "MIT"

import pandas as pd
import time

# runtime hack to import code from a subfolder
import sys
sys.path.insert(0, 'rest_automator/')

import rest_automator 


users = pd.read_csv('data/user_sample.csv').user_id.tolist()

a = rest_automator.Automator(
        user_ids = users,
        ts_min = time.strptime('Oct 1 2015 0:00:00', '%b %d %Y %H:%M:%S'),
        ts_max = time.strptime('Oct 1 2016 0:00:00', '%b %d %Y %H:%M:%S'),
        geo_only = False,
		fname_base = 'sample-')

a.download()