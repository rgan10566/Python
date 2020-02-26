# Author: Ramesh Ganesan
# Purpose: Reads the vulnerabilty scan csv file and breaks them into components to eventually fill up a dynamodb Table
# Date : February, 7
# Assumptions: The vulnerabilty scan is provided as a csv and cleaned up. it contains only needed relevant columns and no heading summarizations or footer summarizations
#
import boto3
import pandas
from boto3.dynamodb.conditions import Key, Attr

# read the csv file using pandas and let it create a dataframe
# Return the dataframe
def read_csv_file():
    txtdf = pandas.read_csv('./BB_Scan_Report_20200203.csv', parse_dates=['First Detected','Last Detected','Date Last Fixed'] )
    return txtdf

# get the unique ip addresses in the file into a list
# use the dataframe to look for unique ip address
# and fill it up in a list and return it back
def get_uniq_ip_addr(df):
    ips=df['IP'].unique()
    return ips

def get_uniq_qid(df):
    ips=df['QID'].unique()
    return ips


# build a dictionary of unique hostnames
# first create an empty datadictionary
# use the list of unique ip addresses passed to check with the dataframe
# to get the dns hostnames
# fill the dictionary with the ip address and the name
def build_host_dt(df):
    lst=get_uniq_ip_addr(df)
    dichost = {}
    for i in range(len(lst)):
        tempdf=df.query('IP == @lst[@i]')
        uniqdns=tempdf['DNS'].unique()
        dichost.update({lst[i]:uniqdns[0]})
    return dichost

# build a dictionary of unique QID
# first create an empty datadictionary
# use the list of unique QID passed to check with the dataframe
# to get the title
# fill the dictionary with the QID and the Title
def build_qid_dt(df):
    lst=get_uniq_qid(df)
    dicqid = {}
    for i in range(len(lst)):
        tempdf=df.query('QID == @lst[@i]')
        uniqtitle=tempdf['Title'].unique()
        dicqid.update({lst[i]:uniqtitle[0]})
    return dicqid
#main
#first call the read_csv_file procedure and get the file into a dataframe using pandas
df = read_csv_file()
#hosts = get_uniq_ip_addr(df)
hostsdt = build_host_dt(df)
qiddt = build_qid_dt(df)

#hosts=get_ip_addr
#hostdt={}

#for i in range(len(hosts)):
#    print(hosts[i],'\n')
#    hostdt.update({hosts[i]:'None'})
#    tempdf=df.query('IP == @hosts[@i]')
#    print(tempdf['DNS'].uni que())
#print(hostdt)
#print(hostsdt)
print(hostsdt['10.1.50.25'])

dynadb = boto3.resource('dynamodb')
try:

    table = dynadb.Table('MQIDS')
except dynadb.exceptions.TableNotFoundException:
    print('Table Does not Exists')

try:

    with table.batch_writer(overwrite_by_pkeys=['QID']) as batch:
        for k,v in qiddt.items():
            batch.put_item(
                Item={
                    'QID': str(k),
                    'Title': str(v)
                    }
                    )
except dynadb.exceptions.ResourceNotFoundException:
    print('Table does not exists')


table = dynadb.Table('MHOSTS')
with table.batch_writer() as batch:
    for k,v in hostsdt.items():
            batch.put_item(
                Item={
                    'IP_ADDR': str(k),
                    'Hostname': str(v)
                    }
                )

response = table.scan(
    FilterExpression=Attr('Hostname').eq('nan')
)

items = response['Items']
print(items)

response = table.query(
    KeyConditionExpression=Key('IP_ADDR').eq('10.109.32.31')
)

items = response['Items']
print(items)
