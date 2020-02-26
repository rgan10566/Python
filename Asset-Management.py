# Author: Ramesh Ganesan
# Purpose: Reads the vulnerabilty scan csv file and breaks them into components to eventually fill up a dynamodb Table
# Date : February, 7
# Assumptions: The vulnerabilty scan is provided as a csv and cleaned up. it contains only needed relevant columns and no heading summarizations or footer summarizations
#
import boto3
import pandas

def create_schema():
    return True

def connect_to_aurora():
    return True

def write_log():
    return True



client = boto3.client('rds')


response = client.describe_db_clusters(
    DBClusterIdentifier='database-2',
)

#print(response)
if len(response['DBClusters']) == 1:
    writer=response['DBClusters'][0]['Endpoint']
else
# for now just assign the 0th item, later create an array and choose one random as a writer
    writer=response['DBClusters'][0]['Endpoint']
