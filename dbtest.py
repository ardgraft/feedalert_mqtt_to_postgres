
import os
import psycopg2
from psycopg2 import errors
from dotenv import load_dotenv




# Load environment variables from the .env file
load_dotenv()





# Retrieve Postgres database credentials from environment variables
host = os.environ.get('POSTGRES_HOST')
dbname = os.environ.get('POSTGRES_DBNAME')
user = os.environ.get('POSTGRES_USER')
password = os.environ.get('POSTGRES_PASSWORD')
port = os.environ.get('POSTGRES_PORT')

conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
cur = conn.cursor()
# swd_imei="01234567890"
# q = "INSERT INTO things (swd_imei, lastupdated, firstseen) VALUES ('" +swd_imei +"', NOW(), NOW())"    
# cur.execute(q)
# r = conn.commit()
# print(r)


q = "INSERT INTO mqtt (timestamp, message, imei, topic, payload, crc) VALUES (NOW(), 'testmessage', '01234567890' , 'test','testpayload', '99999')"
r=cur.execute(q)
print(r)
conn.commit()



