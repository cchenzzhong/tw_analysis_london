#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 12:01:01 2017

functions provide
1. iterate over files in a given directory
2. convert the json into csv

@author: chenzhong
"""

import os
import gzip
import json
import pandas as pd
import numpy as np

dire_name_old = list(['2014-07','2014-08','2014-09','2014-10','2014-11','2014-12','2015-01','2015-02',
             '2015-03','2015-04','2015-05','2015-06','2015-07','2015-08','2015-09',
             '2015-10','2015-11','2015-12','2016-01','2016-02','2016-03',
             '2016-04','2016-05','2016-06','2016-07','2016-08','2016-09',
             '2016-10','2016-11','2016-12','2017-01','2017-02','2017-03','2017-04','2017-05','2017-06'])


#dire_name = list(['2014-07','2014-08','2014-09','2014-10','2014-11','2014-12','2015-01','2015-02',
#             '2015-03','2015-04','2015-05','2015-06','2015-07'])
dire_name = list(['2015-08','2015-09',
             '2015-10','2015-11','2015-12','2016-01','2016-02','2016-03',
             '2016-04','2016-05','2016-06','2016-07','2016-08','2016-09',
             '2016-10','2016-11','2016-12','2017-01','2017-02','2017-03','2017-04','2017-05','2017-06'])

from datetime import datetime
import time

def to_datetime(datestring):
    if pd.isnull(datestring):
        return ''
    elif len(str(datestring))<30:
        return ''
    else:
        return time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(str(datestring),'%a %b %d %H:%M:%S +0000 %Y'))



dire_name = dire_name_old
for name in dire_name:
    directory = '/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-Kenya/'+name
    #directory = '/data/geocomputation/tweets/tweet-extractor-London/'+name
    nrow = len(os.listdir(directory))
    df = pd.DataFrame('', index=np.arange((nrow*4000)), columns=list(['id','text','userid','lat','lon','created_at','location']))    
    count = 0
    filecount = 0
    for filename in os.listdir(directory):
        if filename.endswith(".gz"): 
           filecount = filecount +1
           #print(os.path.join(directory, filename))
           #filename = "/Volumes/FREESPACE/workspace/python/twitter_analysis/2017-06/json_2017-06-13-20.txt.gz"
           print("we move on to another file!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! number %s %s" %(filecount, filename))
           filename = directory + '/'+filename
           tw_id = "haha"
           with gzip.open(filename, 'rb') as f:
               file_content = f.read()
               tweets = file_content.split('\n')
               for tw in tweets:
                   tw_content = tw.split('\t')
                   if(len(tw_content)<2):
                       print("something wrong here#######################!")
                       continue
                   testjson = json.loads(tw_content[1])
                   #print(count)
                   df.loc[count]['id'] = testjson['id']
                   if 'user' not in testjson:
                       df.loc[count]['userid'] = ""
                       continue
                   df.loc[count]['userid'] = testjson['user']['id']
                   #print("user_id is %s" %testjson['user']['id'])
                   coordinates = testjson['coordinates']
                   if coordinates == None:
                       lat = ""
                       lon = ""
                   else:
                       df.loc[count]['lon'] = coordinates['coordinates'][0]
                       df.loc[count]['lat'] = coordinates['coordinates'][1]
                   df.loc[count]['created_at'] = testjson['created_at']
                   df.loc[count]['text'] = testjson['text']
                   if 'place' not in testjson:
                       df.loc[count]['location'] = ""
                       continue
                   else:
                       if testjson['place'] == None:
                           #print("current file is %d %d", filename, tw_id)
                           df.loc[count]['location'] = ""
                           continue
                       else:
                           df.loc[count]['location'] = testjson['place']['name']
                   #new_tw = pd.DataFrame([[tw_id,text,userid,lat,lon,time,place]], columns=list(['id','text','userid','lat','lon','time','location']))
                   #print(new_tw)
                   #df.loc[count] = new_tw
                   count = count +1
                   #print("count of tweet %d" %count)
                   if count > (len(df['userid'])):
                       ext_df = pd.DataFrame('', index=np.arange(1000), columns=list(['id','text','userid','lat','lon','created_at','location']))    
                       df = df.append(ext_df, ignore_index=True)
        else:
            continue
    print("count of tweet %d" %count)
    df = df.loc[(df['id'] != '')&(df['userid'] != '')]
    c = df.created_at.apply(lambda x: to_datetime(x))
    df.created_at = pd.to_datetime(c)
    
    cols = df.columns
    for col in cols[2:-1]:
        df[col] = df[col].astype(str)
        
    df = df[df.location <> 'None']
    csvname = directory + '.csv'  
    df.to_csv(csvname, sep = ',',encoding='utf-8', header = True, index = False)


