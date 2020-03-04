import sys
import boto3
import pymysql
import logging
import pymysql.cursors
import mconfig
import base64
from botocore.exceptions import ClientError
from sqlalchemy import create_engine
import json
import pandas

# this funcuon uses the pandas function to read a csv file and populate the data structure
def read_csv_file():
    # txtdf = pandas.read_csv('/Users/rganesan/Documents/BusinessDocs/BBody/Vulnerability-Security/BB_Scan_Report_20200203.csv', parse_dates=['First Detected','Last Detected','Date Last Fixed'] )
    txtdf = pandas.read_csv('/Users/rganesan/Documents/BusinessDocs/BBody/Vulnerability-Security/BB_Scan_Report_20200203.csv')

    return txtdf

# this is copoied from the aws secret manager code and modified slights to use the secret setup to login to RDS database.
def get_secret():

    secret_name = "appadmin"
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

    if secret != '':
        return secret
    else:
        return decoded_binary_secret

def initconnect(pconfig, log):

    try:
# Connect to the database
#        print("connecting to ",pconfig[0],pconfig[3], pconfig[1],pconfig[2],pconfig[4])
        secret = json.loads(get_secret())
        if secret != '':
            host1=secret["host"]
            user1=secret["username"]
            pass1=secret["password"]
            db1=secret["dbname"]
            port1=secret["port"]
            print(pass1)
            print("fetched from Secret")
        else:
            pass1=pconfig[2]
            print("passed parameter")

        connection = pymysql.connect(host=host1,
                                port=port1,
                                user=pconfig[1],
                                password=pass1,
                                # password=pconfig[2],
                                database=db1)
        # connection = pymysql.connect(host='tablette.cluster-culomlubyiwb.us-west-2.rds.amazonaws.com',
        #                         port=3306,
        #                         user='appadmin',
        #                         password='BB_Tabl3tt3',
        #                         database='tablette')

    except:
        log.error("ERROR: Unexpected error: Could not connect to Aurora instance.")
        return -1

    return connection

def runquery(conn, query, log):

    try:
        with conn.cursor() as cur:
                records = []
                cur.execute(query)
                result = cur.fetchall()
                for row in result:
                    records.append(row)
    except pymysql.MySQLError as e:
        log.error(e)
        return []

    return records

def inserttable(conn, table, data, log):
        print(data)
        try:
            with conn.cursor() as cur:
                    for i in range(1,len(data)):
                        sql="insert into "+table+" ("
                        for k in range(len(data[0])):
                            sql = sql + data[0][k]
                            if k < len(data[0]) -1:
                                sql = sql + ","
                        sql = sql + ") values ("
                        for l in range(len(data[i])):
                            sql = sql + "'"+data[i][l]+"'"
                            if l < len(data[i]) - 1:
                                sql = sql + ","
                        sql = sql + ")"
                        print(sql)
                        query=sql
                        cur.execute(query)
                    conn.commit()
        except pymysql.MySQLError as e:
            log.error(e)
            return False

        return True


def insertvulscan(conn, table, df, log):
        try:
            with conn.cursor() as cur:
                    cols = ",".join([str(i) for i in df.columns.tolist()])

                    for i,row in df.iterrows():
                         sql="insert into RAW_VULSCAN_DATA ("+cols+") VALUES (" + "%s,"*(len(row)-1) + "%s)"
                         print(sql,tuple(row))
                         cur.execute(sql,tuple(row))
                    conn.commit()
        except pymysql.MySQLError as e:
            log.error(e)
            return False

        return True

logger = logging.getLogger()
logger.setLevel(logging.INFO)

conn=initconnect(mconfig.tablette,logger)

if conn != -1:
#    list=[['email','password'],['rame10566@gmail.com','Secret'],['rgan10566@yahoo.com','pesterme']]
    df=read_csv_file()
    insertvulscan(conn,'raw_vulscan_data',df,logger)
#        res=runquery(conn, "select * from users",logger)
#        print(res)
else:
    print("Connection not establised")





# try:
# # Connect to the database
#         connection = pymysql.connect(host='tablette.cluster-culomlubyiwb.us-west-2.rds.amazonaws.com',
#                                 port=3306,
#                                 user='appadmin',
#                                 password='BB_Tabl3tt3',
#                                 database='tablette')
#
# except:
#         logger.error("ERROR: Unexpected error: Could not connect to Aurora instance.")
#         sys.exit()
#
# logger.info("Success: connection to aurora established")
#
# # try:
# #     with connection.cursor() as cursor:
# #         sql = "CREATE TABLE users ( \
# #                 id int(11) NOT NULL AUTO_INCREMENT, \
# #                 email varchar(255) COLLATE utf8_bin NOT NULL, \
# #                 password varchar(255) COLLATE utf8_bin NOT NULL, \
# #                 PRIMARY KEY (id) \
# #                 ) AUTO_INCREMENT=1"
# #         cursor.execute(sql)
#
# try:
#     with connection.cursor() as cursor:
#         sql = "INSERT INTO users ( email, password) values (%s, %s)"
#
#         cursor.execute(sql,('rganesan@beachbody.com','justsomething'))
#
#         connection.commit()
#
# finally:
#     connection.close()
#
# print(mconfig.tablette)
