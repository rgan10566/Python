#importing required Libraries
import sys
import boto3
import pymysql
import logging
import pymysql.cursors
import mconfig
import base64
from botocore.exceptions import ClientError
from operator import itemgetter
from datetime import datetime
#from sqlalchemy import create_engine
import json
import pandas
import time

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
            # print("passed parameter")

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
def read_csv_file(fileloc='/Users/rganesan/Documents/BusinessDocs/BBody/Vulnerability-Security/BB_Scan_Report_20200203.csv'):
    # txtdf = pandas.read_csv('/Users/rganesan/Documents/BusinessDocs/BBody/Vulnerability-Security/BB_Scan_Report_20200203.csv', parse_dates=['First Detected','Last Detected','Date Last Fixed'] )
    txtdf = pandas.read_csv(fileloc,header='infer',skiprows=1)

    return txtdf

## step z: Generic insert into a table query. Automatically takes the table name and build an insert statement. The parameter passed should however match in numbers to the number of coumns.
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
                        # print(sql)
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


##### A Generic run any  query (select, insert, update)
# Purpose: This function uses the pymysql function to run a query passed to it. you can pass any valid query string.
# parameter: Takes three  parameters, conn, query and log. conn is the connection to the database created by calling a different function.
#          : query is the sql query (any valid sql query to fetch data from the connection.
#          : log is used to log messages.
# returns : returns back the count of records it has fetched.
def runquery(conn, query, log):
    result=0
    try:
        with conn.cursor() as cur:
                records = []
                cur.execute(query)
                result = cur.fetchall()
                for row in result:
                    records.append(row)
                conn.commit()

    except pymysql.ProgrammingError as e:
        logger.error("Error: Unexpected Error: Something went wrong in querying METADATA")
        logger.error(e)
        logger.error(sql)
    except pymysql.DataError as d:
        logger.error("Error: Unexpected Error: Something went wrong in querying METADATA")
        logger.error(d)
        logger.error(sql)
    except pymysql.IntegrityError as i:
        logger.error("Error: Unexpected Error: Something went wrong in querying METADATA")
        logger.error(i)
        logger.error(sql)
    except pymysql.OperationalError as o:
        logger.error("Error: Unexpected Error: Something went wrong in querying METADATA")
        logger.error(o)
        logger.error(sql)
    except pymysql.NotSupportedError as n:
        logger.error("Error: Unexpected Error: Something went wrong in querying METADATA")
        logger.error(n)
        logger.error(sql)
    except Exception as x:
        logger.error("Error: Unexpected Error: Something went wrong in Run Querying. The statement did not execute")
        logger.error(x)
        logger.error(result)

    return records

############################# Beginning of metadata functions #################################
################### A function to insert into the metadata table
# Purpose: This function merely queriesthe metadata table and returns te record.
# parameter: Takes these parameters, conn, file, scandata, logger.
#           conn is the connection passed that was already created
#           file is the name of the file. the function can query the metadata table by filename or loadid
#           loadid is the unique record id. if the metadata is already there then that is what should be used to query the table.
#                   we should avoid loading the files multiple time and only one record should exists for a file.
#                   and the same recrods should be updated.
#           logger is the name of the logfile where errors and info are written to.
# returns : returns back the complete record of the metadata values fetched.
#           rundate is the loading date and would always be the date on which the program is first run for a file.
#           scandate is the scan date of the file in the metadata.
#           status would only take values of 'I', 'L', 'C' and denotes the processing of the file (for fresh loads or reloads)
#           loadstats is the different status of the loads and would take value of 'LS','LE','AS','AE','LA','LQ','FL'.
#           reprocessdate is the rundate for reprocessing.
#           reprocessstatus can take the value of 'RS','RE','AS','AE','LA','LQ','FL'
################################################
def querymeta(conn, file, logger, loadelem=0):

    try:
        metarec=[]
        if loadelem == 0 or not loadelem:
            sql="SELECT LOADID,FILENAME,RUNDATE,SCANDATE,STATUS,LOADSTATUS FROM tablette.METADATA where FILENAME ='"+file+"'"
        else:
            sql="SELECT LOADID,FILENAME,RUNDATE,SCANDATE,STATUS,LOADSTATUS FROM tablette.METADATA where LOADID ='"+str(loadelem)+"'"

        metarec=runquery(conn,sql,logger)
        if metarec == []:
            logger.info("No Results returned by the Query")
            logger.info(sql)
        else:
            logger.info("Record found:"+metarec[0][1])

    except pymysql.Error as e:
        logger.error("Error: Unexpected Error: Something went wrong in querying in function querydata")
        logger.error(e)
        logger.error(sql)
    except Exception as x:
        logger.error("Error: Unexpected Error: Something went wrong in processing in querydata")
        logger.error(x)
    return metarec

#################### A function to insert into the metadata table
# Purpose: This function merely inserts into the metadata table. after the completion of the insertion it then selects the same row to keep the loadid.
# parameter: Takes these parameters, conn, file, scandata, logger.
# returns : returns back the loadid.
################################################
def insertmeta(conn, metadata, logger):

    try:
        sql=""
        for k in range(len(metadata)):
            sql = sql +"'"+ metadata[k]+"'"
            if k < len(metadata) - 1:
                sql = sql +","
        sql = "insert into tablette.METADATA (FILENAME, RUNDATE, SCANDATE, LOADSTATUS, STATUS,TIMETAKEN) VALUES ("+sql+")"
#       sql="insert into tablette.METADATA(FILENAME,STATUS,LOADSTATUS,RUNDATE,SCANDATE) VALUES ('"+file+"','I','LS','"+scandate+"','"+datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')+"')"
        r=runquery(conn,sql,logger)
        logger.info("INFO: insert into metadata Executed: "+sql)
        if r == []:
            rec = querymeta(conn, metadata[0], logger, 0)
            if rec != []:
                loadid = rec[0][0]
            else:
                loadid = -1
    except pymysql.Error as e:
        logger.error("Error: Unexpected Error: Something went wrong in insertion or querying in function insertmeta")
        logger.error(e)
        logger.error(sql)
    except Exception as x:
        logger.error("Error: Unexpected Error: Something went wrong in processing in function insertmeta")
        logger.error(x)
    return loadid

#################### A function to update the metadata table
# Purpose: This function updatesthe metadata table. There are different sqls based on the status
# parameter: Takes these parameters, conn, status, loadelem (loadid), timetaken to load,logger.
# returns : returns back the loadid.
################################################
def updatemeta(conn, metadata, logger):

    try:
        retval = 0
        if metadata[5] == 'R':
            sql="Update tablette.METADATA set prevrundate = rundate, prevtimetaken = timetaken, prevstatus = status, status = '"+metadata[5]+"', loadstatus = '"+metadata[4]+"', rundate = '"+metadata[2]+"', scandate = '"+metadata[3]+"', timetaken = "+str(metadata[6])+" where LOADID ="+str(metadata[1])
        else:
            sql="Update tablette.METADATA set status = '"+metadata[5]+"', loadstatus = '"+metadata[4]+"', rundate = '"+metadata[2]+"', scandate = '"+metadata[3]+"', timetaken = "+str(metadata[6])+" where LOADID ="+str(metadata[1])
        rec=runquery(conn,sql,logger)
        retval=metadata[1]

    except pymysql.Error as e:
        logger.error("Error: Unexpected Error: Something went wrong in update in function updatemeta")
        logger.error(e)
        logger.error(sql)
        retval=-1
    except Exception as x:
        logger.error("Error: Unexpected Error: Unable to update metadata")
        logger.error(x)
        retval = -1

    return retval

#################### A function to update the metadata table
# Purpose: This function updatesthe metadata table. There are different sqls based on the status
# parameter: Takes these parameters, conn, status, loadelem (loadid), timetaken to load,logger.
# returns : returns back the loadid.
################################################
def reloadmeta(conn, file, scandate, loadelem, logger):
    metareclist = []
    print("The file is already loaded..")
    inp=input("Confirm Reload file by entering Y/y:")
    if inp == 'Y' or inp == 'y':
        try:
            retval=0
            sql="delete from tablette.VULSCAN_ARCHIVE where loadid = "+str(loadelem)
            r=runquery(conn,sql,logger)
            logger.info("INFO: delete from VULSCAN_ARCHIVE Executed: "+sql)

            sql="delete from tablette.RAW_VULSCAN_DATA where loadid = "+str(loadelem)
            r=runquery(conn,sql,logger)
            logger.info("INFO: delete from RAW_VULSCAN_DATA Executed: "+sql)

            metareclist = [datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'),scandate,'RS','R','0']
            metareclist.insert(0,file)
            metareclist.insert(1,loadelem)

            retval=loadelem
            retval=updatemeta(conn, metareclist, logger)

        except pymysql.Error as e:
            logger.error("Error: Unexpected Error: Something went wrong in delete or  update in function reloadmeta")
            logger.error(e)
            logger.error(sql)
            retval = -1
        except Exception as x:
            logger.error("Error: Unexpected Error: Unable to delete from VULSCAN_ARCHIVE")
            logger.error(x)
            retval = -1
    else:
        logger.error("Warning: The file is already loaded and you choose not to Reload")
        retval=-1
    return retval

########################## check metadata to see if the file is already loaded
# Purpose: The purpose is only to check if the filename passed is already there in the metadata table
# parameter: conn, connection already created to database
#            file, name of the file that needs to be loaded
#            scandate, date of the scan
#            logger, logging file
##############################################
def checkmeta(conn, file, scandate, logger):
    metareclist = []
    try:
        rec = querymeta(conn, file, logger, 0)
        retval = dict()
        if not rec:
            logger.info("No Records found..loading file for the first time")
            metareclist = [datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'),scandate,'LS','I','0']
            metareclist.insert(0,file)
            loadid = insertmeta(conn, metareclist, logger)
            if loadid == -1:
                logger.info("There must have been an issue with inserting into metadata. please check for any errors")
            else:
                retval = ('INSERTED',loadid)
        else:
            if len(rec) > 1:
                logger.error("Something went wrong. Metadata seems to have multiple loads of this file. please correct manually")
                retval = ('MANUAL INTERVENTION NEEDED',0)
            elif len(rec) == 1:
                logger.info(rec)
                stat=rec[0][4]
                lstat=rec[0][5]
                if stat=='C':
                    logger.info("INFO File has been already reloaded")
                    loadid = reloadmeta(conn, file, scandate, rec[0][0], logger)
                    retval = ('RELOADING',loadid)
                    print(retval)
                elif stat=='I' or stat=='R':
                    logger.error("Error: File is in the process of reloading and we cannot reprocess the file again")
                    retval = ('RELOADING ALREADY',0)
                elif stat=='L' or stat=='P':
## We can in the future expand this to only load archive or asset or qid - to be coded later
## for now we pretend that all loads will be a full load and if this is partiall done then it will be manually loaded
                    logger.error("Error: File is partially loaded. Canot reprocess the file again.")
                    retval = ('PARTIALLY LOADED',0)
    except pymysql.Error as e:
        logger.error("Error: Unexpected Error: Something went wrong in update, insert, querying in function reloadmeta")
        logger.error(e)
        logger.error(sql)
        retval = ('ERROR',-1)
    except Exception as x:
        logger.error("ERROR: Unexpected Error: Something went wrong in the processing in function checkmeta")
        logger.error(x)
        retval = ('ERROR',-1)
    return retval

## MAIN ##
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#create connection and use it by passing this to other functions
conn=initconnect(mconfig.tablette,logger)
rundate=datetime.strptime(mconfig.file[2],'%Y-%m-%d %H:%M:%S')
#metadata

metadict=[]
if conn != -1:
    rec=checkmeta(conn,'TEST1_Sev_4_5.csv', '2020-05-21 00:00:00', logger)
    if rec[0] == 'INSERTED':
        print("The METADATA got a new insert")
    elif rec[0] == 'RELOADING':
        print("The METADATA is marked for Reload")
    elif rec[0] == 'PARTIALLY LOADED' or rec[0] == 'RELOADING ALREADY':
        print("The file is partially loaded.")
        print("If you must reload, check the processes running on the database, kill them.")
        print("Manually check and correct the partially loaded data and clear the data in all respective data")
        print("Reset the metadata")
    else:
        print("Error or Manual intervention needed")
    print("Finished")
else:
    print("Connection not establised")
conn.close()
