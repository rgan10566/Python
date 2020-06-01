
#Author: Ramesh Ganesan (RG)
#Contributors:
#
#StartDate: March 2nd
#Last Updated Date: March 25th,2020
#Last Updated by: RG
#
#Purpose: To read a csv file and populate a raw table in AWS Aurora (Tabletter schema on BB Lab)
#
#
# changes to be done:
#   before uploading the file check on the metadata
#   if the metadata already exists for the file then check if it is loaded already
#   if not already loaded then update the metadata for loading Begins
#   if already loaded ask if needs to be reloaded
#   if reload needed then delete earlier vulscan load and reload
#   update the METADATA for reload
#   load the Vulscan
#   update the laodid also into the Vulscan
#   after the raw file load is successful load it into the archive vulscan table
#   load the assets into the master asset table
#   load the qid into thr master table
#   Attributes:
#
#   Status on METADATA
#           I - Initiated load
#           R - Initiated Reload
#           L - Raw Vulscan loaded
#           P - RAW Vulscan reload complete
#           C - Completed
#   Load/Reloadstatus:
#           LS  - New load started
#           RS  - Reload Started
#           LE  - New Load Ennded
#           RE  - Reload ended
#           AS  - Archival Started
#           AE  - Archival Ended
#           LA  - Loading Asset
#           LQ  - Loading QID
#           FL  - Fully Load complete
#           DR  - Deleing Raw Vulscan entries
#           DA  - Deleting Archive Entries
#   Additional status on METADATA
#           AA - New Assets added to the Asset master table but QID not loaded
#           QA - New QID added to the QID master table => this should follow AA. Assets and QID loaded

##

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

########## Initialization Packages ##########


######################## Generic functions ###############################

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
    except pymysql.MySQLError as e:
        log.error("Encountered an Error running the query %s"%query)
        log.error(e)

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
def querymeta(conn, file, loadelem=0, logger):

    try:
        rec=[]
        if loadelem == 0 or not loadelem:
            sql="SELECT LOADID,FILENAME,RUNDATE,SCANDATE,STATUS,LOADSTATUS,REPROCESSDATE,REPROCESSTATUS FROM tablette.METADATA where FILENAME ='"+file+"'"
        else:
            sql="SELECT LOADID,FILENAME,RUNDATE,SCANDATE,STATUS,LOADSTATUS,REPROCESSDATE,REPROCESSTATUS FROM tablette.METADATA where LOADID ='"+str(loadelem)+"'"

        metarec=runquery(conn,sql,logger)
        logger.info("Record found"+rec)
    except:
        logger.error("Error: Unexpected Error: Something went wrong in querying METADATA")
        logger.error(sql)
    return metarec

#################### A function to insert into the metadata table
# Purpose: This function merely inserts into the metadata table. after the completion of the insertion it then selects the same row to keep the loadid.
# parameter: Takes these parameters, conn, file, scandata, logger.
# returns : returns back the loadid.
################################################
def insertmeta(conn, file, scandate, logger):

    try:
        # sql="insert into tablette.METADATA(FILENAME,STATUS) VALUES ('"+file+"','"+scandate+"','"+todate+"','LS')"
        sql="insert into tablette.METADATA(FILENAME,STATUS,LOADSTATUS,RUNDATE,SCANDATE) VALUES ('"+file+"','I','LS','"+scandate+"','"+datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')+"')"
        r=runquery(conn,sql,logger)
        logger.info("INFO: insert into metadata Executed: "+sql)

        rec = querymeta(conn, file, 0, logger)
        loadid = rec[0][0]
    except:
        logger.error("Error: Unexpected Error: Something went wrong in inserting into METADATA")
        logger.error(sql)
    return loadid


#################### A function to update the metadata table
# Purpose: This function updatesthe metadata table. There are different sqls based on the status
# parameter: Takes these parameters, conn, status, loadelem (loadid), timetaken to load,logger.
# returns : returns back the loadid.
################################################
def updatemeta(conn, status, loadelem, timetaken=0.0, logger):

    try:
        retval = 0
        if status == 'RS':
            sql="Update tablette.METADATA set status = '"+status+"', reprocessdate = '"+todate+"' where LOADID ="+str(Loadelem)
            retval=loadelem
        elif status == 'AR':
            sql="Update tablette.METADATA set status = '"+status+"' where LOADID ="+str(Loadelem)
            retval=loadelem
        rec=runquery(conn,sql,logger)
    except:
        logger.error("Error: Unexpected Error: Unable to update metadata")
        logger.error(pymysql.errorsql)
        retval = -1

    return retval

#################### A function to update the metadata table
# Purpose: This function updatesthe metadata table. There are different sqls based on the status
# parameter: Takes these parameters, conn, status, loadelem (loadid), timetaken to load,logger.
# returns : returns back the loadid.
################################################
def reloadmeta(conn, file, scandate, loadelem, rundatelem, logger):

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

            retval=loadelem
            retval=updatemeta(conn, 'RS', loadelem, rundatelem, logger)
        except:
            logger.error("Error: Unexpected Error: Unable to delete from VULSCAN_ARCHIVE")
            logger.error(pymysql.errorsql)
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
    try:
        rec = querymeta(conn, file, 0, logger)
        retval = dict()
        if not rec:
            logger.info("No Records found..loading file for the first time")
            loadid = insertmeta(conn,file, scandate, logger)
            retval = ('INSERTED',loadid)
        else:
            if len(rec) > 1:
                logger.error("Something went wrong. Metadata seems to have multiple loads of this file. please correct manually")
                retval = ('MANUAL INTERVENTION NEEDED',0)
            elif len(rec) == 1:
                logger.info(rec)
                stat=rec[0][4]
                lstat=rec[0][5]
                rstat=rec[0][7]
                if stat='C':
                    logger.info("INFO File has been already reloaded")
                    oadid = reloadmeta(conn,file, scandate, rec[0][0], rec[0][2], 0, logger)
                    retval = ('RELOADING',loadid)
                elif stat=='I' or stat=='R':
                    logger.error("Error: File is in the process of reloading and we cannot reprocess the file again")
                    retval = ('MANUAL INTERVENTION NEEDED',0)
                elseif stat=='L' or stat=='P':
## We can in the future expand this to only load archive or asset or qid - to be coded later
## for now we pretend that all loads will be a full load and if this is partiall done then it will be manually loaded
                    logger.error("Error: File is partially loaded. Canot reprocess the file again.")
                    retval = ('MANUAL INTERVENTION NEEDED',0)
    except:
        logger.error("ERROR: Unexpected Error: Something went wrong in the querying of METADATA Table")
        logger.error(sql)
        retval = ('ERROR',-1)
    return retval

######################### END of Metadata Functions #######################################

######################## Beginning of Load into vulscan table functions ##############################

###### Data handling routines
# Purpose: This function uses the pymysql function to insert into the table RAW_VULSCAN_DATA. The data that needs to be inserted is passed as a pandas data frame. Thefunction prepares the columns from the labels of the data frame
# parameter: Takes four  parameters, conn, table, dataframe  and log. conn is the connection to the database created by calling a different function.
#          : table is the name of the table where data needs to be inserted. this would be RAW_VULSCAN_DATA
#          : df is the data frame that gets populated by pandas and read from a csv file.
#          : log is used to log messages.
# returns  : returns back the count of data inserted.
def insertvulscan(conn, schema, table, df, loadelem, log):
        try:
# when can you start the insert? should there be a check for metadata status?
            rundate=datetime.strftime(datetime.now()
            with conn.cursor() as cur:

                    cols = ",".join([str(i) for i in df.columns.tolist()])
                    count=tcount=0
                    stime=time.time()
                    for i,row in df.iterrows():
                         sql="insert into "+schema+"."+table + "("+cols+", rundate, loadid) VALUES (" + "%s,"*(len(row)-1) + "%s,'"+rundate+"','"+str(loadelem)+"')"
                         # print(sql,tuple(row))
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

###### Data handling routines
# Purpose: This function uses the pymysql function to insert into the table RAW_VULSCAN_DATA. The data that needs to be inserted is passed as a pandas data frame. Thefunction prepares the columns from the labels of the data frame
# parameter: Takes four  parameters, conn, table, dataframe  and log. conn is the connection to the database created by calling a different function.
#          : table is the name of the table where data needs to be inserted. this would be RAW_VULSCAN_DATA
#          : df is the data frame that gets populated by pandas and read from a csv file.
#          : log is used to log messages.
# returns  : returns back the count of data inserted.
def inserttoarchive(conn, schema, stable, ttable, log):
    try:
# 5/4/2020 notes
# when can we start insert into archive? should we check if the status on metadata is correct before insertsing
# do we insert into archive directly after the insert into raw vulscan?
        stime=time.time()
        sql="insert into "+schema+"."+ttable + " as select * from "+schema+"."+stable
        rec=runquery(conn,sql,logger)
        etime=time.time()
        retval=1
        print("Total processed %d records in %s time "%(i+1,time.strftime("%H:%M:%S", time.gmtime(etime-stime))))

    except:
        logger.error("Error: Unexpected Error: Unable to delete from VULSCAN_ARCHIVE")
        logger.error(pymysql.errorsql)
        retval = -1

    return retval

def loadnewassets(conn, rundate, logger):
## identify new assets and add them.
## report the list of newly added ASSETS
    try:
        rec=runquery(conn, "insert into tablette.ASSETS (ip, dns, OS, trackingmethod, scandate) select distinct ip,dns,os, trackingmethod, '"+rundate+"' from tablette.RAW_VULSCAN_DATA v where not exists (select ip from tablette.ASSETS a where a.ip=v.ip)",logger)
    except:
        logger.error("Error: Unknown Error: Unable to insert into ASSETS Table")
        return -1
    return 0

def loadnewqids(conn, rundate, logger):
## identify any new qid that has come up.
## add them to the qid master table with title
    try:
        rec=runquery(conn, "INSERT INTO tablette.QID(QID,TITLE,VULNSTATUS,TYPE,SEVERITY,CVEID,VENDORREFERENCE,PCIVULN,CATEGORY,THREAT,IMPACT,SOLUTION,STATUS,SCANDATE) select distinct QID,TITLE,VULNSTATUS,TYPE,SEVERITY,CVEID,VENDORREFERENCE,PCIVULN,CATEGORY,THREAT,IMPACT,SOLUTION,'New','"+rundate+"' from tablette.RAW_VULSCAN_DATA v where QID is not null and not exists (select qid from tablette.QID a where a.qid=v.qid)",logger)
    except:
        logger.error("Error: Unknown Error: Unable to insert into QID Table")
        return -1
    return 0

# Purpose: This function loads the csv file into the RAW_VULSCAN_DATA table. it calls several other functions.
# parameter: Takes two parameters, conn, logger
#          : Conn is the connection that is created in the main function that gets passed to this function.
#          : logger is the connection to logging function where messages (error/info) gets written to
# logic    : - gets the csv filename and location.
#          : - calls the function read_csv_file that loads the csv file into records
#          : - gets the rundate from the config filelo
#          : - all the function check_meta which inturns the metadata table to see if the file was already loaded.
#          : - before loading, the table has to be truncate, before truncating the data currently in RAW_VULSCAN_DATA should be moved into PREV_VULSCAN_DATA
#          : - After insertipn, then truncate the RAW_VULSCAN_DATA
#          : - Calls insertvulcan function that inserts into RAW_VULSCAN_DATA
# returns  : returns back the count of data inserted.
def loadvulscan(conn, logger):

##  Read from the mconfig file parameter and the date
    file=mconfig.file[0]+mconfig.file[1]
    scandate=mconfig.file[2]

## call read_csv_file to load into records in memory
    df=read_csv_file(file)
    df.fillna("",inplace = True)
##
##  Read from the mconfig file parameter

    # print(file)
    # print(rundate)
##
##  check if the file has already been loaded into the metadata
##  if done, then ask if it needs to be reloaded
##  delete the entries from previous vulscan and reload and reprocess
##  if not, then load the first time and update metadata
##
    starttime=time.time()
    rval = checkmeta(conn,mconfig.file[1], scandate,logger)
    if rval[0] == 'INSERTING' or rval[0] == 'RELOADING':
        loadelem=rval[1]

## check the status once again to make sure of the metadata. if status right then load into Vulscan
## if the metadata has been updated properly then the status should be in LS (Load Start) or RS (Reload start)
        metarec = querymeta(conn, mconfig.file[1], loadelem, logger)
        if len(metarec) == 1 and (metarec[2] == 'LS' or metarec[2] == 'RS'):
            if metarec[2] == 'LS':
                endstate='LE'
            elif merarec[2] == 'RS':
                endstate='RE'
            retval = inserttovulscan(conn, 'Tablette', 'RAW_VULSCAN_DATA', df, loadelem, log)
            endtime=time.time()
            if retval > 0:
## update metadata to indicate that Archive needs to start
                updatemeta(conn, 'AR', loadelem, time.strftime("%H:%M:%S", time.gmtime(endtime-starttime)), logger)
                inserttoarchive(conn, 'Tablette', 'RAW_VULSCAN_DATA',  'VULSCAN_ARCHIVE', logger)
## uodate meatadata to indicate thar archive is completed
                endtime=time.time()
                updatemeta(conn, endstate, loadelem, time.strftime("%H:%M:%S", time.gmtime(endtime-starttime)), logger)

## now before you call to update assets and qid esure that the metadata status shows archive ended.
## then load assets and qid
                metarec = querymeta(conn, mconfig.file[1], loadelem, logger)
                if len(metarec) == 1 and (metarec[2] == endstate:
                    loadnewassets(conn,mconfig.file[1],logger)
                    loadnewqids(conn,mconfig.file[1],logger)
## at the end update status to 'C'
                    endtime=time.time()
                    updatemeta(conn, 'C', loadelem, time.strftime("%H:%M:%S", time.gmtime(endtime-starttime)), logger)


##  after insertion into vulscan tables then new asset need to be loaded with unique asset id.
##  after insertion into vulscan also insert new qid in the vulscan qid master


    else:
        print("Unable to load...please check metadata")


    return 1






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

#create connection and use it by passing this to other functions
conn=initconnect(mconfig.tablette,logger)
rundate=datetime.strptime(mconfig.file[2],'%Y-%m-%d %H:%M:%S')
#old redundant code
# file=mconfig.file[0]+mconfig.file[1]
# rundate=mconfig.file[2]
# print(file)
# print(rundate)

if conn != -1:
# read the CSV file and insert into table
    # Old Redundat Code
    # df=read_csv_file(file)
    # df.fillna("",inplace = True)
    # cnt=runquery(conn,'insert into tablette.PREVIOUS_VULSCAN (ip,dns,team,netbios,trackingmethod,os,ipstatus,qid,title,vulnstatus,\
    # type,severity,port,protocol,fqdn,ssllayer,firstdetected,lastdetected,timesdetected,datelastfixed,\
    # cveid,vendorreference,bugtraqid,threat,impact,solution,exploitability,associatedmalware,results,pcivuln,category) \
    # select ip,dns,team,netbios,trackingmethod,os,ipstatus,qid,title,vulnstatus,type,severity,port,protocol,fqdn,\
    # ssllayer,firstdetected,lastdetected,timesdetected,datelastfixed,cveid,vendorreference,bugtraqid,threat,impact,solution,\
    # exploitability,associatedmalware,results,pcivuln,category, rundate from tablette.RAW_VULSCAN_DATA',logger)
    # print("Previous Vulscan table inserted",cnt)
    # rec = runquery(conn, 'truncate table '+'RAW_VULSCAN_DATA', logger)
    # cnt=insertvulscan(conn,'tablette','RAW_VULSCAN_DATA',df,logger)
    # print("%s rows inserted",cnt)
    #select table count for the RAW_VULSCAN_DATA

#This function loads the vulnerabilty.

    loadvulscan(conn, logger)

#this checks the number of rows loaded now and prints it.
    rec = runquery(conn, 'select count(1) from '+'RAW_VULSCAN_DATA', logger)
    print("Total number of records in RAW_VULSCAN_DATA is %10d"%rec[0])

#create table LATEST_VULSCAN from the uploaded table
    # rec=runquery(conn,'drop table if exists tablette.LATEST_VULSCAN',logger)
    # loadnewasset(conn,mconfig.file[2], logger)
    # loadqid(conn, mconfig.file[2], logger)

else:

    print("Connection not establised")

conn.close()
