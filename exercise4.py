#sample exercise on dictionary
import datetime
from dateutil.tz import *


dict={'Name':'Ramesh','Age':54,'Sex':'Male'}

tdict={'DBClusters': [{'AllocatedStorage': 1, 'AvailabilityZones': ['us-west-2c', 'us-west-2b', 'us-west-2a'], 'BackupRetentionPeriod': 1, 'DatabaseName': '', 'DBClusterIdentifier': 'database-2', 'DBClusterParameterGroup': 'default.aurora5.6', 'DBSubnetGroup': 'default-vpc-b10e9ed7', 'Status': 'available', 'EarliestRestorableTime': datetime.datetime(2020, 2, 12, 10, 32, 17, 269000, tzinfo=tzutc()), 'Endpoint': 'database-2.cluster-culomlubyiwb.us-west-2.rds.amazonaws.com','ReaderEndpoint':'database-2.cluster-ro-culomlubyiwb.us-west-2.rds.amazonaws.com', 'MultiAZ': True, 'Engine': 'aurora', 'EngineVersion': '5.6.10a', 'LatestRestorableTime': datetime.datetime(2020, 2, 14, 1, 11, 50, 144000, tzinfo=tzutc()), 'Port': 3306, 'MasterUsername': 'admin', 'PreferredBackupWindow': '10:18-10:48', 'PreferredMaintenanceWindow': 'fri:09:10-fri:09:40', 'ReadReplicaIdentifiers': [], 'DBClusterMembers': [{'DBInstanceIdentifier': 'database-2-instance-1-us-west-2b', 'IsClusterWriter': False, 'DBClusterParameterGroupStatus': 'in-sync', 'PromotionTier': 1}, {'DBInstanceIdentifier': 'database-2-instance-1', 'IsClusterWriter': True, 'DBClusterParameterGroupStatus': 'pending-reboot', 'PromotionTier': 1}], 'VpcSecurityGroups': [{'VpcSecurityGroupId': 'sg-d26471a8', 'Status': 'active'}, {'VpcSecurityGroupId': 'sg-09970cc3183c32851', 'Status': 'active'}], 'HostedZoneId': 'Z1PVIF0B656C1W', 'StorageEncrypted': True, 'KmsKeyId': 'arn:aws:kms:us-west-2:248415844586:key/1b08bd6e-17c5-4f0f-8cfa-d6a78971cf2f', 'DbClusterResourceId': 'cluster-67E4Z53DMPAZ7CI6TMYM7EOM2U', 'DBClusterArn': 'arn:aws:rds:us-west-2:248415844586:cluster:database-2', 'AssociatedRoles': [], 'IAMDatabaseAuthenticationEnabled': False, 'ClusterCreateTime': datetime.datetime(2019, 12, 17, 2, 1, 33, 890000, tzinfo=tzutc()), 'EngineMode': 'provisioned', 'DeletionProtection': True, 'HttpEndpointEnabled': False, 'ActivityStreamStatus': 'stopped', 'CopyTagsToSnapshot':True,'CrossAccountClone': False}],
'ResponseMetadata': {'RequestId': 'ed56c562-203b-4dc7-b917-8eed871ddde1', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amzn-requestid': 'ed56c562-203b-4dc7-b917-8eed871ddde1', 'content-type': 'text/xml', 'content-length': '3855', 'vary': 'accept-encoding', 'date': 'Fri, 14 Feb 2020 01:14:07 GMT'}, 'RetryAttempts': 0}}

print('My dummy dictionary keys')
#print keys
for k in dict:
    print(k)

print('My dummy dictionary keys using key function')
#print keys
for k in dict.keys():
    print(k)

print('My dummy dictionary values for each key using index')

#print values for each key
for k in dict:
    print(dict[k])

print('My dummy dictionary values for each key using values function')
#print all values
for v in dict.values():
    print(v)

print('My dummy dictionary values length')

#find tje length
l=len(dict)
print(l)

print('Looking for the name key and printing its value')
#look for a key and print its values
if 'Name' in dict:
    print(dict['Name'])

print(tdict['DBClusters'][0]['DBClusterIdentifier'])
#print(tdict['DBClusters'][0]['Endpoint'])
#print(tdict['DBClusters'][0]['ReaderEndpoint'])

for i in range(len(tdict['DBClusters'])):
    print(tdict['DBClusters'][i]['Endpoint'])
