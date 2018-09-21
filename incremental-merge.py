#!/usr/bin/env python
import snowflake.connector
import logging
import boto3
import os
from base64 import b64decode

#set logging level
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#set all of the environment variables including the encrypted password
#variables need to be set up in the aws lambda config
env_snow_pw_encryp = os.environ['snowflake_pw']
env_snow_pw_decryp = boto3.client('kms').decrypt(CiphertextBlob=b64decode(env_snow_pw_encryp))['Plaintext']
env_snow_account = os.environ['snowflake_account']
env_snow_user = os.environ['snowflake_user']
env_snow_role = os.environ['snowflake_role']
env_snow_warehouse = os.environ['snowflake_warehouse']

#query to merge results into target
merge_query = """merge into rpa_demo.public.mike_test_encryp_merge a
using (select num, text from rpa_demo.public.mike_test_encryp) b on a.num = b.num
when matched then update set a.num = b.num, a.text = b.text
when not matched then insert (a.num, a.text) values (b.num, b.text)
;"""

#query to check results of merge query
check_query = """select "number of rows inserted", "number of rows updated" from table(result_scan(last_query_id()));"""

#add lambda injenction point
def lambda_handler(event,context):

    #connect to snowflake data warehouse using env vars
    con = snowflake.connector.connect(
    user = env_snow_user,
    password = env_snow_pw_decryp,
    account = env_snow_account,
    role = env_snow_role,
    warehouse = env_snow_warehouse
    )

    #run the merge query followed immediately by the check query
    cur = con.cursor()
    try:
        cur.execute(merge_query)

        #print the query id in case it needs to be used for review
        print("QueryID: "+cur.sfqid)

        cur.execute(check_query)

        #print the check query results **Important** check query is specific to merge
        results = cur.fetchall()
        for (column1, column2) in results:
            print(str(column1)+" rows inserted, "+str(column2)+" rows updated")

    #check for errors but raise exception to lambda to trigger failure if error
    except snowflake.connector.errors.ProgrammingError as e:
        print("Something has gone horribly wrong with your code and you need to fix it!")
        print("Error {0} ({1}): {2} ({3})".format(e.errno, e.sqlstate, e.msg, e.sfqid))
        raise

    finally:
        cur.close()

    con.close()
