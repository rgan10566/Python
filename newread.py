
#Author: Ramesh Ganesan (RG)
#Contributors:
#
#StartDate: September 30,2020
#Last Updated Date:
#Last Updated by: RG
#
#Purpose: To read a csv file, sort the data and write it back to a text file (csv file) using pandas
#

##

#importing required Libraries
import sys
import logging
import base64
from operator import itemgetter
from datetime import datetime
#from sqlalchemy import create_engine
import json
import pandas
import time

########## Initialization Packages ##########


######################## Generic functions ###############################


### The fulction is supposed to take care of setting the logging
def setlog(logfilename, level):
    return True

#### looks like an empty function
def create_latest_scan():
    return

########################### Read csv file into records
# Purpose: This function uses the pandas function to read a csv file and populate the data structure and return back the datastructure
# parameter: Location of the file with name
# Returns : Populated Datastructure
###########################
def read_csv_file(fileloc='/Users/rganesan/scripts/Oracle_Inventory_Test.csv'):
    # txtdf = pandas.read_csv('/Users/rganesan/Documents/BusinessDocs/BBody/Vulnerability-Security/BB_Scan_Report_20200203.csv', parse_dates=['First Detected','Last Detected','Date Last Fixed'] )
    txtdf = pandas.read_csv(fileloc,header='infer',skiprows=0)

    return txtdf

## MAIN ##
# Purpose: This main code. first creates a logger. Then calls initconnect to connect to the AWS RDS schema. Then it calls the pandas function by passing the location of file to be read.
#        : it then fills all the nan columns with null strings
#        : it then calls the function insertvulscan to insert into the RAW_VULSCAN_DATA table passing the dataframe it got from the read_csv_file function.
#        :
# returns  : returns back the count of data inserted.

# initiate any variables and configurations or open database connections, setup logging
# if all above iniialising are successful, then call loadvulscan
# main
###* call loading; function loadvulscan
#######** call metadata check; function checkmeta
#######** take action if file already loaded.; function actionmeta
#######** else new file insert, insert metadata for the file and mark it as load started; function updatemeta
#######** read csv file into record; function read_csv_file
#######** insert into vulscan table; function insertvulscan
#######** insert any new assets not in asset table in the new vulscan into assets table; function loadnewasset
#######** insert any new qid not already in qid table into qid table; function loadnewqid
#######** mark the load as complete in the metadata; function updatemeta
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

file = '/Users/rganesan/scripts/Oracle_Inventory_Test.csv'
df=read_csv_file(file)
df.fillna("",inplace = True)
##
##
starttime=time.time()
df.sort_values(by=['env','hostname','sid'],inplace=True)
#    sort_csv()
#    print_csv()
#print(df)
# for i,row in df.iterrows():
#     print('{:>40} {:>10} {:>40}'.format(row['hostname'],row['env'],row['sid']))

print('----')
print('all:')
print('\tchildren:')
hostname = ''
env=''
sid=''
for i,row in df.iterrows():
    if env != row['env']:
        env=row['env']
        print('\t\t{}:'.format(env))
        if hostname != row['hostname']:
            hostname=row['hostname']
            print('\t\t\thosts:')
            print('\t\t\t\t{}:'.format(hostname))
            print('\t\t\t\t\tdatabases:')
            if sid != row['sid']:
                sid=row['sid']
                print('\t\t\t\t\t\t-sid:{:>30}\n'.format(sid))
        else:
            sid=row['sid']
            print('\t\t\t\t\t\t-sid:{:>30}'.format(sid))
    else:
        if hostname != row['hostname']:
            hostname=row['hostname']
            print('\t\t\thosts:')
            print('\t\t\t\t{}:'.format(hostname))
            print('\t\t\t\t\tdatabases:')
            if sid != row['sid']:
                sid=row['sid']
                print('\t\t\t\t\t\t-sid:{:>30}'.format(sid))
        else:
            sid=row['sid']
            print('\t\t\t\t\t\t-sid:{:>30}'.format(sid))
    endtime=time.time()
