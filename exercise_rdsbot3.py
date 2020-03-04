# Author: Ramesh Ganesan
# Purpose: Reads the vulnerabilty scan csv file and breaks them into components to eventually fill up a dynamodb Table
# Date : February, 7
# Assumptions: The vulnerabilty scan is provided as a csv and cleaned up. it contains only needed relevant columns and no heading summarizations or footer summarizations
#


# Use this code snippet in your app.
# If you need more information about configurations or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developers/getting-started/python/
#
# import boto3
# import base64
# from botocore.exceptions import ClientError
#
#
# def get_secret():
#
#     secret_name = "database-2"
#     region_name = "us-west-2"
#
#     # Create a Secrets Manager client
#     session = boto3.session.Session()
#     client = session.client(
#         service_name='secretsmanager',
#         region_name=region_name
#     )
#
#     # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
#     # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
#     # We rethrow the exception by default.
#
#     try:
#         get_secret_value_response = client.get_secret_value(
#             SecretId=secret_name
#         )
#     except ClientError as e:
#         if e.response['Error']['Code'] == 'DecryptionFailureException':
#             # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
#             # Deal with the exception here, and/or rethrow at your discretion.
#             raise e
#         elif e.response['Error']['Code'] == 'InternalServiceErrorException':
#             # An error occurred on the server side.
#             # Deal with the exception here, and/or rethrow at your discretion.
#             raise e
#         elif e.response['Error']['Code'] == 'InvalidParameterException':
#             # You provided an invalid value for a parameter.
#             # Deal with the exception here, and/or rethrow at your discretion.
#             raise e
#         elif e.response['Error']['Code'] == 'InvalidRequestException':
#             # You provided a parameter value that is not valid for the current state of the resource.
#             # Deal with the exception here, and/or rethrow at your discretion.
#             raise e
#         elif e.response['Error']['Code'] == 'ResourceNotFoundException':
#             # We can't find the resource that you asked for.
#             # Deal with the exception here, and/or rethrow at your discretion.
#             raise e
#     else:
#         # Decrypts secret using the associated KMS CMK.
#         # Depending on whether the secret is a string or binary, one of these fields will be populated.
#         if 'SecretString' in get_secret_value_response:
#             secret = get_secret_value_response['SecretString']
#         else:
#             decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
#
#
#the database on this does not exists


import boto3
import base64
from botocore.exceptions import ClientError


def get_secret():

    secret_name = "database-2"
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])




client = boto3.client('rds')


response = client.describe_db_clusters(
    DBClusterIdentifier='database-2'
)

#print(response)
if len(response['DBClusters']) == 1:
    writer=response['DBClusters'][0]['Endpoint']
else:
# for now just assign the 0th item, later create an array and choose one random as a writer
    writer=response['DBClusters'][0]['Endpoint']

print(writer)

for a in range(len(response['DBClusters'][0]['DBClusterMembers'])):
    print(response['DBClusters'][0]['DBClusterMembers'][a])
    if response['DBClusters'][0]['DBClusterMembers'][a]['IsClusterWriter'] == True:
        writer=response['DBClusters'][0]['DBClusterMembers'][a]['DBInstanceIdentifier']
    print('Writer',writer)


response = client.describe_db_instances(
    DBInstanceIdentifier=writer
)

print(response)

rds_client=boto3.client('rds-data')


cluster_arn = 'arn:aws:rds:us-west-2:248415844586:cluster:database-2'
secret_arn = 'arn:aws:secretsmanager:us-west-2:248415844586:secret:database-2-mZLGxj'

response=rds_client.execute_statement(resourceArn = cluster_arn, secretArn = secret_arn, database = 'database-2', sql='show tables')

print(response)


#print(response)
#if len(response['DBClusters']) == 1:
#    writer=response['DBClusters'][0]['Endpoint']
#else:
# for now just assign the 0th item, later create an array and choose one random as a writer
#    writer=response['DBClusters'][0]['Endpoint']

#print(writer)
