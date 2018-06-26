#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat May 26 19:43:47 2018

@author: chenzhong
"""

import os
import gzip
import json
import pandas as pd
import numpy as np

import csv
from io import StringIO


import matplotlib.pyplot as plt


from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import numpy as np

import sys
reload(sys)
sys.setdefaultencoding("ISO-8859-1")

# Set ipython's max row display
pd.set_option('display.max_row', 1000)

# Set iPython's max column width to 50
pd.set_option('display.max_columns', 50)

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point




#Kenya
#latitude [6, -6]
#longitude [32, 43]

# London
#latitude [51.2,51.8]
#longitude [-0.7, 0.4]


directory = '/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/tw_15_17.csv'

from datetime import datetime

def convert_toSecond(t):
    return time.mktime(t.timetuple())


def make_point(row):
    return Point(row.lon, row.lat)

tw = pd.read_csv(open(directory,'rU'), encoding='utf-8', engine='c', header = 0, 
                 error_bad_lines=False,parse_dates=True)
tw.columns = ['id','text','userid','lat','lon','created_at','location']

tw['created_at'] =  pd.to_datetime(tw['created_at'], format='%Y-%m-%d %H:%M:%S.%f')

c = tw.created_at.apply(lambda x: convert_toSecond(x))
tw['time'] = c
tw['userid'] = tw['userid'].astype(object)
tw = tw.sort_values(by = 'time', ascending = True)
points = tw.apply(make_point, axis=1)
tw_points = gpd.GeoDataFrame(tw, geometry=points)
tw_points.crs = {'init': 'epsg:4326'}



users_dire = '/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/sel_user1.csv'
sel_user = pd.read_csv(users_dire)

import geopandas as gpd
geo_dire = '/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/london_borough.shp'
london = gpd.read_file(geo_dire)
        
geo_dire_msoa = '/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/london_msoa.shp'
london_msoa = gpd.read_file(geo_dire_msoa)

tw_in_msoa = gpd.sjoin(tw_points, london_msoa, op ='within')

london_msoa = london_msoa.drop(columns=['LA_NAME', 'GEOEAST' ,'GEONORTH', 'POPEAST','POPNORTH', 'AREA_KM2'])

cmap = matplotlib.cm.get_cmap('jet')
norm = matplotlib.colors.Normalize(tw['time'].min(), tw['time'].max())
color_producer = matplotlib.cm.ScalarMappable(norm=norm, cmap=cmap)
rgba = color_producer.to_rgba(tw['time'])


for i in range(100):
    print(i)
    tw_user = tw_points[tw['userid'] == sel_user.iloc[i]['user_id']]
    rgba_s = rgba[tw['userid'] == sel_user.iloc[i]['user_id']]
    f, ax = plt.subplots(1, figsize=(10, 8))
    ax = london.plot(axes=ax, color='lightgrey', linewidth=0.5, edgecolor='white', figsize=(15,5))
    ax.set_ylim(51.2,51.80)
    ax.set_xlim(-0.7, 0.4)
    # Pass ax=ax to the second layer
    tw_user.plot(markersize=10, color=rgba_s, alpha=1, axes=ax)
    ax.axis('off')

    '''
    ax = fig.add_subplot(111, projection='3d')
    ax.scatter(tw_user['lat'], tw_user['lon'], tw_user['time'], c = tw_user['time'])
    ax.set_xlabel('X Label')
    ax.set_ylabel('Y Label')
    ax.set_zlabel('Z Label')
    '''
    #plt.show() 
    plt.savefig('fig/'+str(tw_user.iloc[i]['userid']) + '.png')


    
    
    plt.scatter(tweets.lat, tweets.lon)










