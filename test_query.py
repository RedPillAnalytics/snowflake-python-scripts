#!/usr/bin/env python
import snowflake.connector

# Connecting to Snowflake using the default authenticator
ctx = snowflake.connector.connect(
user='<your_user_name>',
password='<your_password>',
account='<your_account_name>',
region = '<your_region>'
role = <'your role'>
)

cur = ctx.cursor()
try:
    cur.execute("SHOW USERS IN ACCOUNT;")
    cur.execute("""SELECT "login_name" from table(result_scan(last_query_id()));""")
    resultsone = cur.fetchall()
    for (col1) in resultsone:
        print(filter(col1)[0])
finally:
    cur.close()

cursecond = ctx.cursor()
try:
    cursecond.execute("""select "NUM","TEXT" from rpa_demo.public.mike_test_encryp;""")
    results = cursecond.fetchall()
    for (col1, col2) in results:
        print('{0}, {1}'.format(col1, col2))
finally:
    cursecond.close()

ctx.close()
