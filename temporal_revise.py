#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun May 27 00:33:58 2018

@author: chenzhong
"""

import os
import pandas as pd
import datetime

from datetime import datetime
import time

def to_datetime(datestring):
    if pd.isnull(datestring):
        return ''
    elif len(str(datestring))<30:
        return ''
    else:
        return time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(str(datestring),'%a %b %d %H:%M:%S +0000 %Y'))


directory = '/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london' 
for filename in os.listdir(directory):
    if filename.endswith(".csv"): 
       #print(os.path.join(directory, filename))
       #filename = "/Volumes/FREESPACE/workspace/python/twitter_analysis/2017-06/json_2017-06-13-20.txt.gz"
       print("we move on to another file!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! number %s %s" %(filecount, filename))
       filename = directory + '/'+filename
       tw_id = "haha"
       df = pd.read_csv(filename, encoding='utf-8')
       c = df.time.apply(lambda x: to_datetime(x))
       df.time = pd.to_datetime(c)

       cols = df.columns
       for col in cols[2:-1]:
           df[col] = df[col].astype(str)
           
       df = df[df.location <> 'None']
       colnames = ['id', 'text', 'userid', 'lat', 'lon', 'created_at', 'location']
       df.columns = colnames
       csvname = filename + '.csv'  
       df.to_csv(csvname, sep = ',',encoding='utf-8', header = True, index = False)
