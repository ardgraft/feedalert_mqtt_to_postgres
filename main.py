import paho.mqtt.client as mqtt
import uuid
import datetime
import psycopg2
from psycopg2 import errors
import queue

import logging
import time
import binascii
import crcmod.predefined
import os
import threading
from telit import telitHandler
from dotenv import load_dotenv
# from pushover import Client
from logging.handlers import TimedRotatingFileHandler


# Load environment variables from the .env file
load_dotenv()
telit = telitHandler()

# Access the variables
MQTT_HOST = os.environ.get("MQTT_HOST")
MQTT_PORT = os.environ.get("MQTT_PORT")
MQTT_CLIENT_ID = os.environ.get("MQTT_CLIENT_ID")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_ENV = os.environ.get("MQTT_ENV")
PUSHOVER_API_TOKEN = os.environ.get("PUSHOVER_API_TOKEN")
PUSHOVER_USER_KEY = os.environ.get("PUSHOVER_USER_KEY")

# push = Client(PUSHOVER_USER_KEY, api_token=PUSHOVER_API_TOKEN)
# push.send_message("Hello! This is a test message from Python.", title="Test Message")

# Create a CRC-32 checksum object
crc32 = crcmod.predefined.Crc('crc-32')



logger = logging.getLogger("MyLogger")
logger.setLevel(logging.INFO)

# Create a handler that writes log messages to a file, rotating the log file
# at a specified interval - for example, at midnight every day
handler = TimedRotatingFileHandler(
    "mqtt_to_postgres.log", 
    when="midnight", 
    interval=1,
    backupCount=7  # Keep 7 days of logs
)

# Define the format for the log entries
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

# Create a handler for logging to the console (screen)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Add the console handler to the logger
logger.addHandler(console_handler)

logger.info("Starting up...")

# Create a queue for messages that need to be written to the database
write_queue = queue.Queue()


def create_crc(data):
    # Convert data to bytes and calculate the CRC-32 checksum
    crc32.update(data.encode('utf-8'))
    checksum = crc32.digest()

    # Convert checksum to an ASCII-encoded hexadecimal string
    hex_checksum = binascii.hexlify(checksum).decode('ascii')

    # DOES NOT EXISTR! > Reset the checksum object for the next calculation
    # crc32.reset()

    return hex_checksum

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT broker")
        # push.send_message("MQTT connected", title="MQTT Connected")
        client.subscribe("#")
    else:
        # push.send_message("MQTT connection failed. Retrying...", title="MQTT Connect Failed")
        logger.warning("Connection failed. Retrying in 5 seconds...")
        

def on_disconnect(client, userdata, rc):
    if rc != 0:
        # push.send_message("MQTT connection lost. Reconnecting...", title="MQTT Connection Lost")
        logger.warning("Connection lost. Reconnecting...")
    
    print("Disconnected, attempting to reconnect...")
    try:
        client.reconnect()
    except Exception as e:
        print(f"Reconnect failed: {e}")

def on_message(client, userdata, msg):
    try:
        # Create CRC from message and topic
        my_date = datetime.datetime.now() 
        
        serial_number = my_date.strftime("%Y%m%d%H%M%S%f")
        imei = msg.topic.split("/")[1]
        payload = msg.payload.decode("utf-8")
        crc = create_crc(msg.topic + payload + serial_number)
        t = uuid.uuid4().hex
        crc = crc + t
        # Add message and CRC to write queue if CRC is unique
        write_queue.put((my_date, imei, msg.topic, payload, crc))
        
    except Exception as e:
        raise
        logger.error("Error handling MQTT message: %s", str(e))
        exit(1)

def write_to_database():
    global conn, telit
    
    while True:
        try:

                
            cur = conn.cursor()
            while True:
                try:
                    # Get the next message from the write queue
                    message = write_queue.get(block=False)
                except queue.Empty:
                    break
                try:
                    # Insert the message into the database
                    env = MQTT_ENV
                    last_slash_index = message[2].rfind('/')
                    # Slice the string from the character after the last '/'
                    topic = message[2][last_slash_index + 1:]
                    imei = message[1]

                    if any(s in topic for s in ["swc","swd","mqtt_"]):
                        device_type = "old"
                    else:
                        device_type = "swx"
                    
                    cur.execute("INSERT INTO mqtt (timestamp, imei, message, payload, crc, env, topic) VALUES (%s, %s, %s, %s, %s, %s, %s)", (*message,env,topic))
                    if device_type == "old":
                        cur.execute("SELECT swd_imei FROM things WHERE swd_imei = %s", (message[1],))
                    else:
                        cur.execute("SELECT swd_imei FROM things WHERE imei = %s", (message[1],))
                    
                    cur.fetchone()
                    
                    if cur.rowcount > 0:
                        if device_type == "old":
                            q = "UPDATE things SET " + topic + "='"+ message[3] +"', lastupdated=NOW() WHERE swd_imei = '"+ message[1] +"'"
                        else:
                            q = "UPDATE things SET " + topic + "='"+ message[3] +"', lastupdated=NOW() WHERE imei = '"+ message[1] +"'"
                        cur.execute(q)

                    else:
                        if device_type == "old":
                            q = "INSERT INTO things (swd_imei, " + topic + "," + "lastupdated) VALUES ('" + message[1] +"', '" + message[3] + "', NOW())"
                        else:
                            q = "INSERT INTO things (imei, " + topic + "," + "lastupdated) VALUES ('" + message[1] +"', '" + message[3] + "', NOW())"
                        cur.execute(q)

                except errors.UndefinedColumn as e:
                        conn.rollback()
                        sql="ALTER TABLE things ADD COLUMN " + topic + " TEXT;"
                        cur.execute(sql)
                        conn.commit()
                        
                        
                
                except Exception as e:
                    raise
                    # Handle unique constraint violations separately
                    
            conn.commit()
            
        except Exception as e:
            
            # push.send_message("Error writing to database. Retrying...", title="Database Write Error")
            logger.error("Error writing to database: %s", str(e))
            time.sleep(1)
            exit(1)
            
def opendatabase():
    global conn
    
    # Retrieve Postgres database credentials from environment variables
    host = os.environ.get('POSTGRES_HOST')
    dbname = os.environ.get('POSTGRES_DBNAME')
    user = os.environ.get('POSTGRES_USER')
    password = os.environ.get('POSTGRES_PASSWORD')
    port = os.environ.get('POSTGRES_PORT')
    
    try:
        conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    except Exception as e:
            logger.error("Error connecting to database: %s", str(e))
            # push.send_message("Error connecting to database. Exiting...", title="Database Connection Error")
            exit(1)
    return conn

# def publisher_thread():
#     global conn, telit
#     cur = conn.cursor()
    
#     while True:
#         q = "SELECT swd_imei FROM things WHERE swd_publishdebug is NULL limit 1"
#         cur.execute(q)
#         row = cur.fetchall()

#         for r in row:
#             telit.setThingAttr(r[0], "swd_publishdebug", "1")
#             logger.info(f"Setting swd_publishdebug to 1 for {r[0]}")
#             time.sleep(5)
        
#         time.sleep(360)
        
#         q = "SELECT swd_imei FROM things WHERE swd_publishattributes is NULL limit 1"
#         cur.execute(q)
#         row = cur.fetchall()

#         for r in row:
#             telit.setThingAttr(r[0], "swd_publishattributes", "1")
#             logger.info(f"Setting swd_publishaatributes to 1 for {r[0]}")
#             time.sleep(5)
            
#         time.sleep(360)

if __name__ == "__main__":
    
    logger.info("Starting message queue thread")
    # Create a separate thread to process messages in the write queue
    global conn
    conn = opendatabase()
    
    write_thread = threading.Thread(target=write_to_database, daemon=True)
    write_thread.start()

    # Connect to the MQTT broker
    logger.info("Setting up MQTT parameters")
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    
    # thread = threading.Thread(target=publisher_thread)
    # thread.daemon = True  # This ensures the thread will be killed when the main program exits
    # thread.start()
    
    
    while True:
        try:
            logger.info("Connecting to MQTT broker")
            client.connect(MQTT_HOST, int(MQTT_PORT), 60)
            client.loop_forever()
        except Exception as e:
            logger.error("Error connecting to MQTT broker: %s", str(e))
            exit(1)
