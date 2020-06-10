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

def loadnewqids(schema, qtable, stable ):
## identify any new qid that has come up.
## add them to the qid master table with title
    try:
        stime=time.time()
        collist=['QID','TITLE','VULNSTATUS','TYPE','SEVERITY','CVEID','VENDORREFERENCE','PCIVULN','CATEGORY','THREAT','IMPACT','SOLUTION','RUNDATE']
        cols = ",".join(collist)

        sql="INSERT INTO "+schema+"."+qtable+"("+cols+",STATUS)"+" select distinct "+cols+", 'New' from "+schema+"."+stable+" v where QID is not null and not exists (select qid from "+schema+"."+qtable+" a where a.qid=v.qid)"
#        rec=runquery(conn, sql,logger)
        print(sql)
    except pymysql.Error as e:
        logger.error("Database Error in the insert into vulscan table")
        logger.error(e)
        logger.error(sql)
        retval = -1
    except Exception as e:
        logger.error("Logical or other Errors in the insert into vulscan table")
        logger.error(e)
        retval = -1
    return 0

##main
loadnewqids('tablette','QID','RAW_VULSCAN_DATA')
