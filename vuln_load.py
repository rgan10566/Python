
#Author: Ramesh Ganesan (RG)
#Contributors:
#
#StartDate: March 2nd
#Last Updated Date: March 4th,2020
#Last Updated by: RG
#
#Purpose: To read a csv file and populate a raw table in AWS Aurora (Tabletter schema on BB Lab)
#
#

#importing required Libraries
import sys
import boto3
import pymysql
import logging
import pymysql.cursors
import mconfig
import base64
from botocore.exceptions import ClientError
#from sqlalchemy import create_engine
import json
import pandas
import time

# Purpose: This funcuon uses the pandas function to read a csv file and populate the data structure and return back the datastructure
# parameter: Location of the file with name
# returns : Populated Datastructure

def read_csv_file(fileloc='/Users/rganesan/Documents/BusinessDocs/BBody/Vulnerability-Security/BB_Scan_Report_20200203.csv'):
    # txtdf = pandas.read_csv('/Users/rganesan/Documents/BusinessDocs/BBody/Vulnerability-Security/BB_Scan_Report_20200203.csv', parse_dates=['First Detected','Last Detected','Date Last Fixed'] )
    txtdf = pandas.read_csv(fileloc)

    return txtdf

# Purpose: This funcuon uses the aws boto function to read a secret key. The secret key read here allows to connect to the AWS schema Tablette and access tables in the schema.
# parameter: Takes two parameters, secret_name and region_name. secret_name is the name of the secret key and the region the secret is stored in.
# returns : returns back the populates string depennding if the secret is a binary secret or not.
def get_secret(secret_name="appadmin",region_name="us-west-2"):

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

# Purpose: This function uses the pymysql function to connect to the database and returns back the connection.
# parameter: Takes two parameters, pconfig and log. pconfig is a dummy configuration list that is fetched from a file. This is used only if the secret key call was not sucessful.
#          : log is the general logger that prints messages.
# returns : returns back the connection.
def initconnect(pconfig, log):

    try:
# Connect to the database
#        print("connecting to ",pconfig[0],pconfig[3], pconfig[1],pconfig[2],pconfig[4])
        secret = json.loads(get_secret('appadmin','us-west-2'))
        if secret != '':
            host1=secret["host"]
            user1=secret["username"]
            pass1=secret["password"]
            db1=secret["dbname"]
            port1=secret["port"]
            # print(host1,user1,pass1,db1,port1)
            # print("fetched from Secret")
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

# Purpose: This function uses the pymysql function to run a query passed to it. you can pass any valid query string.
# parameter: Takes three  parameters, conn, query and log. conn is the connection to the database created by calling a different function.
#          : query is the sql query (any valid sql query to fetch data from the connection.
#          : log is used to log messages.
# returns : returns back the count of records it has fetched.
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

# Purpose: This function uses the pymysql function to insert into a table. The data that needs to be inserted is passed as a list with the first row in the list as the columns of the table that needs to be populated.
# parameter: Takes four  parameters, conn, table, data  and log. conn is the connection to the database created by calling a different function.
#          : table is the name of the table where data needs to be inserted
#          : data is a list of data that needs to be inserted. the first row of the data will contain the columns that needs to be populated.
#          : log is used to log messages.
# returns  : returns back true or false.
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
                        print(sql)
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
            print(sql)
            log.error(e)
            return False

        return True

# Purpose: This function uses the pymysql function to insert into the table RAW_VULSCAN_DATA. The data that needs to be inserted is passed as a pandas data frame. Thefunction prepares the columns from the labels of the data frame
# parameter: Takes four  parameters, conn, table, dataframe  and log. conn is the connection to the database created by calling a different function.
#          : table is the name of the table where data needs to be inserted. this would be RAW_VULSCAN_DATA
#          : df is the data frame that gets populated by pandas and read from a csv file.
#          : log is used to log messages.
# returns  : returns back the count of data inserted.
def insertvulscan(conn, schema, table, df, log):
        try:

            with conn.cursor() as cur:

                    cols = ",".join([str(i) for i in df.columns.tolist()])
                    count=tcount=0
                    stime=time.time()
                    for i,row in df.iterrows():
                         sql="insert into "+schema+"."+table + "("+cols+") VALUES (" + "%s,"*(len(row)-1) + "%s)"
                         print(sql,tuple(row))
                         cur.execute(sql,tuple(row))
                         count=count+1
                         if count >= 500:
                             tcount=tcount+count
                             etime=time.time()
                             print("Processed %d records - Total processed %d in %s time "%(count, i+1, time.strftime("%H:%M:%S", time.gmtime(etime-stime))))
                             conn.commit()
                             count=0
                    conn.commit()
                    etime=time.time()
                    print("Total processed %d records in %s time "%(i+1,time.strftime("%H:%M:%S", time.gmtime(etime-stime))))
        except pymysql.MySQLError as e:
            log.error(e)
            print("Error while insert %s"%str(count))
            return count

        # except pymysql.Warning as e:
        #     log.error(e)
        #     print("Warning while insert %s"%str(i))
        #     continue

        return i+1

def setlog(logfilename, level):
    return True


def create_latest_scan():

    return

# Purpose: This main code. first creates a logger. Then calls initconnect to connect to the AWS RDS schema. Then it calls the pandas function by passing the location of file to be read.
#        : it then fills all the nan columns with null strings
#        : it then calls the function insertvulscan to insert into the RAW_VULSCAN_DATA table passing the dataframe it got from the read_csv_file function.
#        :
# returns  : returns back the count of data inserted.

logger = logging.getLogger()
logger.setLevel(logging.INFO)

conn=initconnect(mconfig.tablette,logger)
file=mconfig.file[0]+mconfig.file[1]
print(file)
if conn != -1:
# read the CSV file and insert into table
    df=read_csv_file(file)
    df.fillna("",inplace = True)
    rec = runquery(conn, 'truncate table '+'RAW_VULSCAN_DATA', logger)
    cnt=insertvulscan(conn,'tablette','RAW_VULSCAN_DATA',df,logger)
    print("%s rows inserted",cnt)
#select table count for the RAW_VULSCAN_DATA
    rec = runquery(conn, 'select count(1) from '+'RAW_VULSCAN_DATA', logger)
    print("Total number of records in RAW_VULSCAN_DATA is %10d"%rec[0])

#create table LATEST_VULSCAN from the uploaded table
    rec=runquery(conn,'drop table tablette.LATEST_VULSCAN',logger)

    rec=runquery(conn,'create table tablette.LATEST_VULSCAN as select *, now() RUN_DATE FROM RAW_VULSCAN_DATA',logger)
    print("Latest Vulscan table created",rec)

    rec=runquery(conn, 'insert into tablette.ASSETS (ip, dns) select distinct ip,dns from tablette.LATEST_VULSCAN v where not exists (select ip from tablette.ASSETS a where a.ip=v.ip)',logger)

    rec = runquery(conn, 'select count(1) from '+'LATEST_VULSCAN', logger)
    print("Total number of records in LATEST_VULSCAN is %10d"%rec[0])
else:

    print("Connection not establised")



# earlier test code
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
