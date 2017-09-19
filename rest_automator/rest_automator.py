__author__ = "Sam Maurer"
__date__ = "September 18, 2017"
__license__ = "MIT"


import json
import os
import time
import zipfile
from datetime import datetime as dt
from TwitterAPI import TwitterAPI, TwitterError

from keys import *   # keys.py in same directory


OUTPUT_PATH = 'data/'  # output path relative to the script calling this class
FNAME_BASE = 'retrospective-'  # default filename prefix (file count will be appended)
SUFFIX_DIGITS = 3  # min digits for filename suffix (2=01, 3=001, etc)
ROWS_PER_FILE = 500000  # 500k tweets is about 1.6 GB uncompressed
DELAY = 5.0  # initial reconnection delay in seconds
RATE_LIMIT_INTERVAL = 1.0  # pause between queries, in seconds
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
        self.fcount = 0  # count of output files
        self.nusers = len(user_ids)  # total number of user id's
        self.ucount = 0  # count of user id's
        self._reset_delay()
    
    
    def _reset_delay(self):
        self.delay = DELAY/2
        return
    
    
    def download(self):
        '''Initialize and manage the download process'''
        
        try: 
            for uid in self.user_ids:
                try:
                    self.process_user(uid)

                except TwitterError.TwitterRequestError:
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
    
        self.ucount += 1
        status_ts = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        print(status_ts + " User " + str(self.ucount) + " of " + str(self.nusers))
        
        last_post_id = None
        last_ts = None
        
        while True:
            r = self.page_tweets(uid, last_post_id)
        
            item_count = 0
            for item in r.get_iterator():
                item_count += 1
                try:
                    last_post_id = item['id']
                    last_ts = time.strptime(item['created_at'], 
                            '%a %b %d %H:%M:%S +0000 %Y')
                    if (last_ts >= self.ts_min) and (last_ts < self.ts_max):
                        if (not self.geo_only):
                            self._save_item(item)
                        elif (self.geo_only and
                                (item.get('coordinates') or item.get('place'))):
                            self._save_item(item)
                    
                except ValueError:
                    # A necessary key is missing, either because the item is an API 
                    # status message (save it) or a badly formed tweet (skip it); see
                    # http://geduldig.github.io/TwitterAPI/errors.html
                    if 'message' in item:
                        self._save_item(item)
                    else:
                        continue
        
            time.sleep(RATE_LIMIT_INTERVAL)

            if (item_count == 0):
                # No more tweets left from a user (we've either paged through all of them,
                # reached the API limit, or the entire batch is native retweets)
                msg = {"custom_status": "reached_limit", "user_id": uid,
                       "limit_date": time.strftime('%Y-%m-%d', last_ts)}
                self._save_item(msg)
                return
            
            if (last_ts < self.ts_min):
                # We've passed the time window of interest
                # Warning: will break too soon if there are native retweets
                return
        
    
    def page_tweets(self, user_id, last_post_id=None):
        '''Page through a user's tweets in reverse chronological order'''
        
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

    
    def _save_item(self, item):
        '''Save item to disk'''

        # initialize new output file if tweet count is 0
        if (self.tcount == 0):
            self.fcount += 1
            suffix = ('{0:0'+str(SUFFIX_DIGITS)+'d}').format(self.fcount)
            self.fpath = OUTPUT_PATH + self.fname_base + suffix + '.json'
            self.f = open(self.fpath, 'w')

        # save to file, or skip if the data is incomplete or has encoding errors
        try:
            self.f.write(json.dumps(item) + '\n')
            self.tcount += 1
            # TO DO: does this break if the first tweet in a new file can't be written?
        except:
            return

        # close the output file when it fills up
        if (self.tcount >= ROWS_PER_FILE):
            self._close_files()
            self.tcount = 0


    def _close_files(self):
        '''Close output file and compress if requested'''
        
        if (self.fcount > 0):
            # close the output file
            self.f.close()
        
            if COMPRESS:
        
                # zip the output file
                with zipfile.ZipFile(self.fpath + '.zip', 'w', zipfile.ZIP_DEFLATED) as z:
                    arcname = self.fpath.split('/')[-1]  # name for file inside archive
                    z.write(self.fpath, arcname)
            
                # delete the uncompressed copy
                os.remove(self.fpath)     
    
