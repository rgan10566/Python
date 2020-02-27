import sys
import boto3
import pymysql
import logging
import pymysql.cursors
import mconfig


def initconnect(pconfig, log):

    try:
# Connect to the database
        print("connecting to ",pconfig[0],pconfig[3], pconfig[1],pconfig[4])
        connection = pymysql.connect(host=pconfig[0],
                                port=pconfig[3],
                                user=pconfig[1],
                                password=pconfig[2],
                                database=pconfig[4])

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
        sql="insert into "+table+" ("
        try:
            with conn.cursor() as cur:
                    for i in 1..len(data):
                        for k in range(len(data[0])):
                            sql = sql + data[0][i]
                        sql = sql + ") values"
                        for l in range(len(data[i])):
                            sql = sql + data[i][l]
                        sql = sql + ")"
                    print(sql)
                    query=sql
                    cur.execute(query)
        except pymysql.MySQLError as e:
            log.error(e)
            return False

        return True

logger = logging.getLogger()
logger.setLevel(logging.INFO)

conn=initconnect(mconfig.tablette,logger)

if conn != -1:
    if inserttable(conn,"users",[['email','password'],['rame10566@gmail.com','Secret'],['rgan10566@yahoo.com','pesterme']],logger):
        res=runquery(conn, "select * from users",logger)
        print(res)
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
