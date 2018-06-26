#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun May 27 22:37:59 2018

@author: chenzhong
"""

#find the userid to be extracted.

import os
import gzip
import json
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt


from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import numpy as np

pd.set_option('display.float_format', lambda x: '%.f' % x)

all_user = pd.read_table('/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/users_15_17.csv', header = None)
all_user.columns = ['user_id']

user_15_q3 = pd.read_table('/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/users_15_q3.csv', header = None, sep = ',') 
user_15_q3.columns = ['user_id','rec_15q3','loc_15q3']
user_15_q3['user_id'] = user_15_q3['user_id'].astype(object)
user_15_q3['user_id'] = user_15_q3['user_id'].astype(str)
user_15_q3['user_id'] = user_15_q3['user_id'].map(lambda x: x.rstrip('.0'))
user_15_q3 = user_15_q3.reindex()
    
    
user_15_q4 = pd.read_table('/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/users_15_q4.csv', header = None, sep = ',')
user_15_q4.columns = ['user_id','rec_15q4','loc_15q4']
user_15_q4['user_id'] = user_15_q4['user_id'].astype(object)
user_15_q4['user_id'] = user_15_q4['user_id'].astype(str)
user_15_q4['user_id'] = user_15_q4['user_id'].map(lambda x: x.rstrip('.0'))
user_15_q4 = user_15_q4.reindex()

user_16_q1 = pd.read_table('/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/users_16_q1.csv', header = None, sep = ',')
user_16_q1.columns = ['user_id','rec_16q1','loc_16q1']
user_16_q1['user_id'] = user_16_q1['user_id'].astype(object)
user_16_q1['user_id'] = user_16_q1['user_id'].astype(str)
user_16_q1['user_id'] = user_16_q1['user_id'].map(lambda x: x.rstrip('.0'))
user_16_q1 = user_16_q1.reindex()
    
user_16_q2 = pd.read_table('/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/users_16_q2.csv', header = None, sep = ',')
user_16_q2.columns = ['user_id','rec_16q2','loc_16q2']
user_16_q2['user_id'] = user_16_q2['user_id'].astype(object)
user_16_q2['user_id'] = user_16_q2['user_id'].astype(str)
user_16_q2['user_id'] = user_16_q2['user_id'].map(lambda x: x.rstrip('.0'))
user_16_q2 = user_16_q2.reindex()
    
user_16_q3 = pd.read_table('/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/users_16_q3.csv', header = None, sep = ',')
user_16_q3.columns = ['user_id','rec_16q3','loc_16q3']
user_16_q3['user_id'] = user_16_q3['user_id'].astype(object)
user_16_q3['user_id'] = user_16_q3['user_id'].astype(str)
user_16_q3['user_id'] = user_16_q3['user_id'].map(lambda x: x.rstrip('.0'))
user_16_q3 = user_16_q3.reindex()
    
user_16_q4 = pd.read_table('/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/users_16_q4.csv', header = None, sep = ',')
user_16_q4.columns = ['user_id','rec_16q4','loc_16q4']
user_16_q4['user_id'] = user_16_q4['user_id'].astype(object)
user_16_q4['user_id'] = user_16_q4['user_id'].astype(str)
user_16_q4['user_id'] = user_16_q4['user_id'].map(lambda x: x.rstrip('.0'))
user_16_q4 = user_16_q4.reindex()
    
user_17_q1 = pd.read_table('/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/users_17_q1.csv', header = None, sep = ',')
user_17_q1.columns = ['user_id','rec_17q1','loc_17q1']
user_17_q1['user_id'] = user_17_q1['user_id'].astype(object)
user_17_q1['user_id'] = user_17_q1['user_id'].astype(str)
user_17_q1['user_id'] = user_17_q1['user_id'].map(lambda x: x.rstrip('.0'))
user_17_q1 = user_17_q1.reindex()
    
user_17_q2 = pd.read_table('/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/users_17_q2.csv', header = None, sep = ',')
user_17_q2.columns = ['user_id','rec_17q2','loc_17q2']
user_17_q2['user_id'] = user_17_q2['user_id'].astype(object)
user_17_q2['user_id'] = user_17_q2['user_id'].astype(str)
user_17_q2['user_id'] = user_17_q2['user_id'].map(lambda x: x.rstrip('.0'))
user_17_q2 = user_17_q2.reindex()


userids = user_15_q3['user_id']
userids = userids.append(user_15_q4['user_id'])
userids = userids.append(user_16_q1['user_id'])
userids = userids.append(user_16_q2['user_id'])
userids = userids.append(user_16_q3['user_id'])
userids = userids.append(user_16_q4['user_id'])
userids = userids.append(user_17_q1['user_id'])
userids = userids.append(user_17_q2['user_id'])
userids = userids.unique()

userids = pd.DataFrame(userids)
userids.columns = ['user_id']
userids = userids.reindex()
len(userids)
#right_index=True


test = pd.merge(userids, user_15_q3, on=['user_id'] , how = 'left')
test = test.reindex()
len(test)

test = pd.merge(test, user_15_q4, on=['user_id'] , how = 'left')
test = test.reindex()
len(test)

test = pd.merge(test, user_16_q1, on=['user_id'] , how = 'left')
test = test.reindex()
len(test)

test = pd.merge(test, user_16_q2, on=['user_id'] , how = 'left')
test = test.reindex()
len(test)

test = pd.merge(test, user_16_q3, on=['user_id'] , how = 'left')
test = test.reindex()
len(test)

test = pd.merge(test, user_16_q4, on=['user_id'] , how = 'left')
test = test.reindex()
len(test)

test = pd.merge(test, user_17_q1, on=['user_id'] , how = 'left')
test = test.reindex()
len(test)

test = pd.merge(test, user_17_q2, on=['user_id'] , how = 'left')
test = test.reindex()
len(test)

test0 = test[['user_id','rec_15q3','rec_15q4',\
              'rec_16q1','rec_16q2','rec_16q3','rec_16q4',\
              'rec_17q1','rec_17q2',\
              'loc_15q3','loc_15q4',\
              'loc_16q1','loc_16q2','loc_16q3','loc_16q4',\
              'loc_17q1','loc_17q2']]

test1 = test0.fillna(0)

col = test1.columns[1:]
for i in col:
    test1[i] = test1[i].astype(int)


test1['avg_rec'] = np.mean(test1.iloc[:,1:9], axis = 1)
test1['avg_loc'] = np.mean(test1.iloc[:,9:17], axis = 1)
test1['min_loc'] = np.min(test1.iloc[:,9:17], axis = 1)
test1['max_loc'] = np.max(test1.iloc[:,9:17], axis = 1)
test1['min_rec'] = np.min(test1.iloc[:,1:9], axis = 1)
test1['max_rec'] = np.max(test1.iloc[:,1:9], axis = 1)

test1 = test1[test1['min_loc']>0]



test1['avg_15q3'] = test1['rec_15q3']/test1['loc_15q3']
test1['avg_15q4'] = test1['rec_15q4']/test1['loc_15q4']
test1['avg_16q1'] = test1['rec_16q1']/test1['loc_16q1']
test1['avg_16q2'] = test1['rec_16q2']/test1['loc_16q2']
test1['avg_16q3'] = test1['rec_16q3']/test1['loc_16q3']
test1['avg_16q4'] = test1['rec_16q4']/test1['loc_16q4']
test1['avg_17q1'] = test1['rec_17q1']/test1['loc_17q1']
test1['avg_17q2'] = test1['rec_17q2']/test1['loc_17q2']

sel = test1

def remove_std(df, attr):
    #keep only the ones that are within +3 to -3 standard deviations 
    df = df[np.abs(df[attr]-df[attr].mean())<=( 5 *df[attr].std())]
    return df

def remove_quantile(df,attr, min_q, max_q):
    df = df[(df[attr] < df[attr].quantile(max_q)) & (df[attr] > df[attr].quantile(min_q))]
    return df

colnames = sel.columns[-8:]
for index in range(len(colnames)):
    #sel = sel[sel[colnames[index]]>1]
    sel = remove_std(sel, colnames[index])

sel0 = remove_quantile(sel, 'avg_rec', 0.94,1)
len(sel0)

sel = remove_std(sel, 'avg_loc')
sel = remove_std(sel, 'max_rec')
sel1 = sel[sel['min_rec']>4]


colnames = sel.columns[1:17]
for index in range(len(colnames)):
    #sel = sel[sel[colnames[index]]>1]
    sel = remove_quantile(sel, colnames[index],0.75,1)
    
    
colnames = sel.columns[1:-4]
for index in range(len(colnames)):
    #sel = sel[sel[colnames[index]]>1]
    sel = remove_std(sel, colnames[index])
    
    

    
colnames = sel.columns[9:16]
for index in range(len(colnames)):
    sel = sel[sel[colnames[index]]>1]

    
sel0['user_id'].to_csv('/Volumes/FREESPACE/workspace/python/twitter_analysis/data/tweet-extractor-london/sel_user1.csv',header = True, index = False)
    
    
sel = remove_quantile(sel,colnames[index],0.9, 0.9999999999999999999999)

names = '('
for n in range(len(sel)):
    names = names + '\''+ sel.iloc[n]['user_id'] + '\',' 

#connect to mysql
import mysql.connector
from mysql.connector import errorcode

cnx = mysql.connector.connect(user='root', password='111111',
                              host='localhost',
                              database='kenya_tw',
                              charset = 'utf8mb4')

cursor = cnx.cursor(buffered=True)


query = ("SELECT * from kenya_tw.active_user ")
cursor.execute(query)

df = pd.read_sql(query, cnx)
cursor.close()
cnx.close()
    


######

#connect to mysql
import mysql.connector
from mysql.connector import errorcode

cnx = mysql.connector.connect(user='root', password='111111',
                              host='localhost',
                              database='kenya_tw',
                              charset = 'utf8mb4')

cursor = cnx.cursor(buffered=True)
query = ("SELECT * from kenya_tw.active_user ")
cursor.execute(query)

df = pd.read_sql(query, cnx)
cursor.close()
cnx.close()

df['avg_rec'] = np.mean(df.iloc[:,1:5], axis = 1)
df['avg_loc'] = np.mean(df.iloc[:,5:9], axis = 1)
df['sum_rec'] = np.sum(df.iloc[:,1:5], axis = 1)

#select these 
df_sel = df.loc[(df['avg_rec']>1) & (df['avg_loc'] >1)]

def remove_std_right(df, attr):
    #keep only the ones that are within +3 to -3 standard deviations 
    df = df[np.abs(df[attr]-df[attr].mean())<=( 0.5 *df[attr].std())]
    return df

def convert_toSecond(t):
    return time.mktime(t.timetuple())

df_sel = remove_std_right(df_sel, 'avg_rec')
df_sel = remove_std_right(df_sel, 'avg_loc')


plt.scatter(df_sel.avg_rec, df_sel.avg_loc)

cnx = mysql.connector.connect(user='root', password='111111',
                              host='localhost',
                              database='kenya_tw',
                              charset = 'utf8mb4')

cursor = cnx.cursor(buffered=True)
df_sel = df_sel.sort_values(by=['avg_rec'],ascending=False)

tweets_active = []

for index in range(100):
    print(index)
    query1 = ("SELECT * from kenya_tw.tweets_kenya_14 where userid = " + df_sel.iloc[index]['userid'] + " and lat <> '' ")
    query2 = ("SELECT * from kenya_tw.tweets_kenya_15 where userid = " + df_sel.iloc[index]['userid'] + " and lat <> '' ")
    query3 = ("SELECT * from kenya_tw.tweets_kenya_16 where userid = " + df_sel.iloc[index]['userid'] + " and lat <> '' ")
    query4 = ("SELECT * from kenya_tw.tweets_kenya_17 where userid = " + df_sel.iloc[index]['userid'] + " and lat <> '' ")
    cursor.execute(query1)
    tweets = pd.read_sql(query1, cnx)
    cursor.execute(query2)
    tweets = tweets.append(pd.read_sql(query2, cnx))
    cursor.execute(query3)
    tweets = tweets.append(pd.read_sql(query3, cnx))
    cursor.execute(query4)
    tweets = tweets.append(pd.read_sql(query4, cnx))
    c = tweets.created_at.apply(lambda x: convert_toSecond(x))
    tweets['time'] = c
    tweets['lat'] = tweets['lat'].astype(float)
    tweets['lon'] = tweets['lon'].astype(float)
    if index == 0:
        tweets_active = tweets
    else:
        tweets_active = tweets_active.append(tweets)

cursor.close()
cnx.close() 





