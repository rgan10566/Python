

import sys
import boto3
import pymysql
import logging
import pymysql.cursors



def initialize(sconfig):
    sconfig.db_host='tablette-1.culomlubyiwb.us-west-2.rds.amazonaws.com'
    sconfig.db_user='app_admin'
    sconfig.db_password='BB_Tabl3tt3'
    sconfig.db_port='3306'
    sconfig.db_name='tablette'
    return True


    # def open_connection(self):
    #     """Connect to MySQL Database."""
    #     try:
    #         if self.conn is None:
    #             self.conn = pymysql.connect(self.host,
    #                                         user=self.username,
    #                                         passwd=self.password,
    #                                         db=self.dbname,
    #                                         connect_timeout=5)
    #     except pymysql.MySQLError as e:
    #         logging.error(e)
    #         sys.exit()
    #     finally:
    #         logging.info('Connection opened successfully.')
    #
    # def run_query(self, query):
    #     """Execute SQL query."""
    #     try:
    #         self.open_connection()
    #         with self.conn.cursor() as cur:
    #             if 'SELECT' in query:
    #                 records = []
    #                 cur.execute(query)
    #                 result = cur.fetchall()
    #                 for row in result:
    #                     records.append(row)
    #                 cur.close()
    #                 return records
    #             else:
    #                 result = cur.execute(query)
    #                 self.conn.commit()
    #                 affected = f"{cur.rowcount} rows affected."
    #                 cur.close()
    #                 return affected
    #     except pymysql.MySQLError as e:
    #         print(e)
    #     finally:
    #         if self.conn:
    #             self.conn.close()
    #             self.conn = None
    #             logging.info('Database connection closed.')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
# Connect to the database
        connection = pymysql.connect(host='tablette.cluster-culomlubyiwb.us-west-2.rds.amazonaws.com',
                                port=3306,
                                user='appadmin',
                                password='BB_Tabl3tt3',
                                database='tablette')

except:
        logger.error("ERROR: Unexpected error: Could not connect to Aurora instance.")
        sys.exit()

logger.info("Success: connection to aurora established")

# try:
#     with connection.cursor() as cursor:
#         sql = "CREATE TABLE users ( \
#                 id int(11) NOT NULL AUTO_INCREMENT, \
#                 email varchar(255) COLLATE utf8_bin NOT NULL, \
#                 password varchar(255) COLLATE utf8_bin NOT NULL, \
#                 PRIMARY KEY (id) \
#                 ) AUTO_INCREMENT=1"
#         cursor.execute(sql)

try:
    with connection.cursor() as cursor:
        sql = "INSERT INTO users ( email, password) values (%s, %s)"

        cursor.execute(sql,('rganesan@beachbody.com','justsomething'))

        connection.commit()
        
finally:
    connection.close()
