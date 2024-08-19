import datetime
import psycopg2
import crcmod.predefined
import os
import uuid
import binascii
from dotenv import load_dotenv
import sys

# Create a CRC-32 checksum object
crc32 = crcmod.predefined.Crc('crc-32')
# Load environment variables from the .env file
load_dotenv()




def create_crc(data):
    # Convert data to bytes and calculate the CRC-32 checksum
    crc32.update(data.encode('utf-8'))
    checksum = crc32.digest()

    # Convert checksum to an ASCII-encoded hexadecimal string
    hex_checksum = binascii.hexlify(checksum).decode('ascii')

    # DOES NOT EXISTR! > Reset the checksum object for the next calculation
    # crc32.reset()

    return hex_checksum

def create_serial_number(data):
    my_date = datetime.datetime.now() 
    serial_number = my_date.strftime("%Y%m%d%H%M%S%f")
    crc = create_crc(data)
    t = uuid.uuid4().hex
    return crc + t
    

host = os.environ.get('POSTGRES_HOST')
dbname = os.environ.get('POSTGRES_DBNAME')
user = os.environ.get('POSTGRES_USER')
password = os.environ.get('POSTGRES_PASSWORD')
MQTT_ENV = os.environ.get('MQTT_ENV')
print("Create new CRC's for each row in the database.")
print("So we can then ensure the index for crc is unique.")
print("Starting up...")
# Number of records to update at a time

# Check if there are any additional command-line arguments
if len(sys.argv) > 1:
    # Print all arguments except the script name
    for i, arg in enumerate(sys.argv[1:], 1):
        select_size = int(arg)
else:
    select_size = 1000

print(f"Record batch size: {select_size}")

print("Connecting to database and obtaining total records to update.")
conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
cur = conn.cursor()
cur.execute("SELECT count(crc) from mqtt where length(crc) < 10;")
tr = cur.fetchone()[0]
print(f"Total records to update: {tr}")

cur.execute("SELECT crc, timestamp, imei, message FROM mqtt where length(crc) < 10 limit " + str(select_size))
things = cur.fetchall()

# Keep total number of records updated
td = 0

while len(things) > 0:
    for n in things:
        # Create CRC from message and topic
        my_date = datetime.datetime.now() 
        serial_number = my_date.strftime("%Y%m%d%H%M%S%f")
        crc = n[0]
        imei = n[2]
        payload = n[3]
        timestamp = n[1]
        
        crc = create_crc(payload + crc + serial_number)
        t = uuid.uuid4().hex
        crc = crc + t
        
        q = "UPDATE mqtt SET crc = '" + crc + "', env='" + MQTT_ENV + "' WHERE imei = '" + imei + "' AND timestamp = '" + str(timestamp) + "' AND message='" + payload + "';"
        cur.execute(q)
        
        
    conn.commit()
    td += select_size
    print(f"{my_date} - Updated {len(things)} records. {td} of {tr} updated. {round(td/tr*100,2)}% complete.")    
    
    cur.execute("SELECT crc, timestamp, imei, message FROM mqtt where length(crc) < 10 limit " + str(select_size))
    things = cur.fetchall()
    