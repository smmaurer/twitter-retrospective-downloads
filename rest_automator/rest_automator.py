__author__ = "Sam Maurer"
__date__ = "September 17, 2017"
__license__ = "MIT"


import json
import os
import time
import zipfile
from datetime import datetime as dt
from TwitterAPI import TwitterAPI

from keys import *   # keys.py in same directory


OUTPUT_PATH = 'data/'  # output path relative to the script calling this class
FNAME_BASE = 'retrospective-'  # default filename prefix (timestamp will be appended)
ROWS_PER_FILE = 500000  # 500k tweets is about 1.6 GB uncompressed
DELAY = 5.0  # initial reconnection delay in seconds
RATE_LIMIT = 1.0  # pause between queries, in seconds
COMPRESS = False  # whether to zip the resulting data file


class Automator(object):

    def __init__(
            self,
            user_ids,
            ts_min,
            ts_max,
            geo_only = False,
            fname_base = FNAME_BASE ):
    
        self.api = TwitterAPI(consumer_key, consumer_secret, 
                              access_token, access_token_secret)

        self.user_ids = user_ids
        self.ts_min = ts_min
        self.ts_max = ts_max
        self.geo_only = geo_only
        self.fname_base = fname_base

        self.t0 = None  # initialization time
        self.fpath = ''  # output filepath
        self.f = None  # output file
        self.tcount = 0  # tweet count in current file
        self._reset_delay()
    
    
    def _reset_delay(self):
        self.delay = DELAY/2
        return
    
    
    def page_tweets(self, user_id, last_post_id=None):
        '''Request the next batch of tweets'''
        
        endpoint = 'statuses/user_timeline'
        if not last_post_id:
            # First request for this user
            params = {'user_id': user_id, 
                'count': 200, 
                'include_rts': 'false' }
        else:
            # Subsequent requests
            params = {'user_id': user_id, 
                'count': 200, 
                'max_id': last_post_id - 1, 
                'include_rts': 'false' }
        
        return self.api.request(endpoint, params)

    
    def download(self):
        '''Initialize and manage the download process'''
        
        try: 
            for uid in self.user_ids:
                try:
                    self.process_user(uid)

                except TwitterRequestError:
                    # Continue with next user, but with increasing delays in case there's
                    # some other issue
                    self.delay = self.delay * 2
                    print("\n" + str(dt.now()))
                    print("{}: {}".format(type(e).__name__, e))
                    print("Continuing after {} sec. delay".format(self.delay))
                    time.sleep(self.delay)
                    continue
                    
            self._close_files()

        except KeyboardInterrupt:
            try:
                self._close_files()
            except:
                pass
            print
            return
    

    def process_user(self, uid):
        '''Download posts for one user'''
    
        last_post_id = None
        last_ts = None
        
        while True:
            r = self.page_tweets(uid, last_post_id)
        
            response_len = 0
            for item in r.get_iterator():
                response_len += 1
                try:
                    last_post_id = item['id']
                    last_ts = time.strptime(item['created_at'], 
                            '%a %b %d %H:%M:%S +0000 %Y')
                    if (last_ts >= self.ts_min) & (last_ts < self.ts_max):
                        if (not self.geo_only):
                            self._save_item(item)
                        elif (self.geo_only & (item['coordinates'] | item['place'])):
                            self._save_item(item)
                    
                except ValueError:
                    # A necessary key is missing, either because it's a status message 
                    # or the tweet is badly formed
                    if 'message' in item:
                        self._save_item(item)
                    else:
                        continue
        
            time.sleep(RATE_LIMIT)

            if (response_len == 0):
                # No more tweets left from a user
                return
            
            if (last_ts < self.ts_min):
                # We've passed the time window of interest
                # Warning: will break too soon if there are native retweets
                return
        
    
    def _save_item(self, item):
        '''Save item to disk'''

        # initialize new output file if tweet count is 0
        if (self.tcount == 0):
            ts = dt.now().strftime('%Y%m%d-%H%M%S')
            self.fpath = OUTPUT_PATH + self.fname_base + ts + '.json'
            self.f = open(self.fpath, 'w')

        # save to file, or skip if the data is incomplete or has encoding errors
        try:
            self.f.write(json.dumps(item) + '\n')
            self.tcount += 1
        except:
            return

        # close the output file when it fills up
        if (self.tcount >= ROWS_PER_FILE):
            self._close_files()
            self.tcount = 0


    def _close_files(self):
        '''Close output file and compress if requested'''
        
        # close the output file
        self.f.close()
        
        if COMPRESS:
        
            # zip the output file
            with zipfile.ZipFile(self.fpath + '.zip', 'w', zipfile.ZIP_DEFLATED) as z:
                arcname = self.fpath.split('/')[-1]  # name for file inside archive
                z.write(self.fpath, arcname)
            
            # delete the uncompressed copy
            os.remove(self.fpath)     
    
