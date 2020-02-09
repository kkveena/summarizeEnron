#!/usr/bin/env python
# coding: utf-8

# # 1. To get ---"person", "sent", "received"--- as csv for Problem 1

# This Program will take approx ~10 Minutes to run
import sys
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

#Load the data
dfEnron = pd.read_csv(str(sys.argv[-1]),header=None,names=['time','message','sender','recipients',
                                                                       'topic','mode'])
# Convert Sender and Recipients as STR and convert it to lower case for comparision
dfEnron['sender']=dfEnron['sender'].astype(str).map(lambda x:x.lower())
dfEnron['recipients']=dfEnron['recipients'].astype(str).map(lambda x:x.lower())

# Below Method counts number of recipients
def getRecepientCount(recepients):
    cnt=recepients.count('|')
    if cnt and cnt > 0:
        cnt += 1
    else:
        cnt =1
    return cnt

#Count email sent by counting receipients to whom email has been sent
dfEnron['sent']=dfEnron['recipients'].astype(str).map(lambda x:getRecepientCount(x))

# Create colume for collecting received count
dfEnron['received']=''

# For a given Sender , get count of recipients
def getColListAsValWithSender(sender,colVal):
    cnt = colVal.count(sender)
    return cnt

# Get All recipients as List
recptList = str(dfEnron['recipients'].values.tolist())

# Below method will get Target columns - as required by Problem 1
def prepareTargetResult(recptList,dfEnron):
    # Cache to ensure that already retrieved sender count is not recalculated
    cache= {}

    for i in range(len(dfEnron.index)):
        sender = dfEnron.iloc[i]['sender']
        if sender not in cache:
            cnt = getColListAsValWithSender(sender,recptList)
            cache[sender]=cnt
        else:
            cnt = cache[sender]
        
        dfEnron.loc[i,'received']=cnt
    return dfEnron

# We will pass RecipientList and Dataframe and it will populate data frame
dfRawResult = prepareTargetResult(recptList,dfEnron)

#Group to find and sort top Senders
dfGrouped = dfRawResult.groupby(['sender','received']).agg({'sent':'sum'}).sort_values(by=['sent'],ascending=False).reset_index()
dfGrouped = dfGrouped[['sender','sent','received']]
#Collect Problem 1 result
dfGrouped.to_csv("dfProblem1Result.csv",index=False,encoding='utf8')

# # 2. Visualization of the most prolific senders

# Get top 3 senders - we will create a timegraph of email sent by them
topSenders = dfGrouped.loc[0:2,'sender']
topSendersList = topSenders.values.tolist()

#Get Dataframe of top 5 senders
dfTopSenders=dfRawResult.loc[dfEnron['sender'].isin(topSendersList)]
dfTopSenders.reset_index(drop=True,inplace=True)

# Convert Unix Timestamp to Date
dfTopSenders.loc[:,'dateSent'] = dfTopSenders['time'].map(lambda x :
                                                          datetime.utcfromtimestamp(int(x/1000)).strftime('%Y-%m-%d'))

# Select Specific Elements for graph
dfSenderGraph = dfTopSenders[['sender','sent','dateSent']]

# Group it by Sender
dfSenderGraphGrouped = dfSenderGraph.groupby(['sender','dateSent']).agg({'sent':'sum'}).sort_values(by=['sender','dateSent']).reset_index()
dfSenderGraphGrouped['dateSent'] = pd.to_datetime(dfSenderGraphGrouped['dateSent'])
dfSenderGraphGrouped['sender'] = dfSenderGraphGrouped['sender'].astype(str)

# Plot the Graph
plt.rc('xtick',labelsize=15)
plt.rc('ytick',labelsize=15)
plt.figure(figsize=(30, 15))
for i in range(len(topSendersList)):
    dfPlot =dfSenderGraphGrouped[dfSenderGraphGrouped['sender'] == topSendersList[i]]
    dfPlot.set_index('dateSent')['sent'].plot()
plt.legend(topSendersList,fontsize=25,loc='upper left')
plt.xlim(['2000-09-01', '2001-09-01'])
plt.xlabel("Date",fontsize=25)
plt.ylabel("Daily Email Sent",fontsize=25, labelpad=15)
plt.title("Daily Email Sent by Top 3 Users", y=1.02, fontsize=30);
plt.savefig('Problem2Solution.png')

# # Problem 3 - Who are unique people / email contacts who contacted top 3 senders across a defined time interval (2000-09-01 and 2001-09-01)

# For a given sender, find unique people who contacted them
def findUniqueContacts(df,sender):
    uniqueContacts = set()
    for i in range(len(df.index)):
        rcpts = str(df.iloc[i]['recipients'])
        if (rcpts.find(str(sender)) >=0):
            snder = str(df.iloc[i]['sender'])
            uniqueContacts.add(snder)
    if uniqueContacts:
        return len(uniqueContacts)
    else:
        return 0

# We will enrich original dataframe with date field - to take a slice for top 3 senders
dfEnron.loc[:,'dateSent'] = dfEnron['time'].map(lambda x :
                                                          datetime.utcfromtimestamp(int(x/1000)).strftime('%Y-%m-%d'))

# Get DataFrame between a date range 
dfTarget = dfEnron[(dfEnron['dateSent'] >= '2000-09-01') &
                       (dfEnron['dateSent'] <= '2001-09-01')]

#Create a data frame to collect the result
dfCollect = pd.DataFrame(columns=['sender','unqCount'])

# collect the result for top Senders
for i in range(len(topSendersList)):
    dfCollect.loc[i,'sender']=topSendersList[i]
    dfCollect.loc[i,'unqCount']=findUniqueContacts(dfTarget,topSendersList[i])
plt.figure(figsize=(50, 10))
dfCollect.plot(kind='bar',x='sender',y='unqCount',figsize=(25, 10),legend=False)
plt.xlabel("Top Senders",fontsize=25)
plt.ylabel("Unique Contacts of Top Senders",fontsize=25, labelpad=15)
plt.title("Unique contacts of Top 3 senders between 2000-09-01 and 2001-09-01", y=1.02, fontsize=20)
plt.savefig('Problem3Solution.png')




