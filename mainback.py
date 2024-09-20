import os
import sys
import uuid
import time
import queue
import signal
import logging
import datetime
import psycopg2
import requests
import binascii
import threading
import sentry_sdk
import crcmod.predefined
import paho.mqtt.client as mqtt

from psycopg2 import errors
from telit import telitHandler
from dotenv import load_dotenv
from logtail import LogtailHandler




# Handle signals for script stop
def handle_termination_signals(signum, frame):
    logger.error(f"Signal received at line {frame.f_lineno} in {frame.f_code.co_filename}")
    logger.critical(f"Received signal {signum}. Stopping script.")
    sys.exit(0)

# Trap common stop signals
signal.signal(signal.SIGINT, handle_termination_signals)  # Handle Ctrl+C
signal.signal(signal.SIGTERM, handle_termination_signals)  # Handle termination

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
BETTERSTACK_TOKEN = os.environ.get("BETTERSTACK_TOKEN")
SENTRY_DSN=os.environ.get("SENTRY_DSN")
BETTERSTACK_HEARTBEAT_URL=os.environ.get("BETTERSTACK_HEARTBEAT_URL")
DEBUG_MODE = os.environ.get("DEBUG_MODE")

print(f"DEBUG_MODE: {DEBUG_MODE}")

# if DEBUG_MODE != "True":
#     # Setup Sentry
#     sentry_sdk.init(
#         dsn=SENTRY_DSN,
#         # Set traces_sample_rate to 1.0 to capture 100%
#         # of transactions for tracing.
#         traces_sample_rate=1.0,
#         # Set profiles_sample_rate to 1.0 to profile 100%
#         # of sampled transactions.
#         # We recommend adjusting this value in production.
#         profiles_sample_rate=1.0,
#         release="feedalert_mqtt_to_postgresmyapp@1.0.0",
#     )

# Create a CRC-32 checksum object
crc32 = crcmod.predefined.Crc('crc-32')

# To keep track of the last heartbeat time. this is used to check if the script has been running for more than 1 minute and send a heartbeat to betterstack. 
# The check is done only when a MQTT message arrives. Hence, this is to verify MQTT messages are flowing.
last_heartbeat_time = time.time()

if DEBUG_MODE != "True":
    handler = LogtailHandler(source_token=BETTERSTACK_TOKEN)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    logger.addHandler(handler)
    

    # logger = logging.getLogger('screen_logger')
    # # Set the minimum logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    # logger.setLevel(logging.DEBUG)
    # # reate a StreamHandler to log to the console (screen)
    # console_handler = logging.StreamHandler()
    # # Set the logging level for the handler
    # console_handler.setLevel(logging.DEBUG)
    # # Create a formatter for the log messages
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # # Attach the formatter to the handler
    # console_handler.setFormatter(formatter)
    # # Add the handler to the logger
    # # logger.addHandler(console_handler)
    # logger.info("Starting up...")
else:
    # Create a logger
    logger = logging.getLogger('screen_logger')

    # Set the minimum logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    logger.setLevel(logging.DEBUG)

    # Create a StreamHandler to log to the console (screen)
    console_handler = logging.StreamHandler()

    # Set the logging level for the handler
    console_handler.setLevel(logging.DEBUG)

    # Create a formatter for the log messages
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Attach the formatter to the handler
    console_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(console_handler)

    logger.info("Starting up...")

# Create a queue for messages that need to be written to the database
write_queue = queue.Queue()


def check_and_send_heartbeat():
    """
    This function checks if 60 seconds (1 minute) have passed since the last heartbeat.
    If the time difference is greater than or equal to 60 seconds, it sends a heartbeat to BetterStack.

    Global Variables:
    last_heartbeat_time (int): The timestamp of the last heartbeat sent to BetterStack.

    Returns:
    None
    """

    global last_heartbeat_time


    # Get the current time
    current_time = time.time()
    
    # Check if 60 seconds (1 minute) has passed since the last heartbeat
    if current_time - last_heartbeat_time >= 60:
        send_heartbeat()
        # Update the last heartbeat time
        last_heartbeat_time = current_time
    


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
        logger.error("MQTT Connection failed.")
        sys.exit(1)
        

def on_disconnect(client, userdata, rc):
    if rc != 0:
        # push.send_message("MQTT connection lost. Reconnecting...", title="MQTT Connection Lost")
        logger.warning("MQTT connection lost. Reconnecting...")
    
    logger.warning("MQTT disconnected, attempting to reconnect...")
    try:
        client.reconnect()
    except Exception as e:
        logger.critical("Disconnected, unable to reconnect. Failing. Error message: %s", str(e))
        sys.exit(1)

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
        logger.error("Error handling MQTT message: %s", str(e))
        

def write_to_database():
    global conn, telit
    
    # Call the function to send the heartbeat
    
   
    while True:
        try:

                
            cur = conn.cursor()
            while True:
                
                if DEBUG_MODE != "True":
                    check_and_send_heartbeat()

                try:
                    # Get the next message from the write queue
                    message = write_queue.get(block=False)
                    if DEBUG_MODE == "True":
                        logger.info("Processing message: %s", str(message))

                except queue.Empty:
                    break
                try:
                    # Insert the message into the database
                    env = MQTT_ENV
                    last_slash_index = message[2].rfind('/')
                    # Slice the string from the character after the last '/'
                    topic = message[2][last_slash_index + 1:]
                    imei = message[1]
                    
                    if DEBUG_MODE != "True":
                        cur.execute("INSERT INTO mqtt (timestamp, imei, message, payload, crc, env, topic) VALUES (%s, %s, %s, %s, %s, %s, %s)", (*message,env,topic))
                   
                    
                    device_type = ""

                    cur.execute("SELECT swd_imei FROM things WHERE swd_imei = %s", (message[1],))
                    # Fetch the result
                    result = cur.fetchone()
                    # Check if a row was returned and if the IMEI matches
                    if result and result[0] == message[1]:
                        device_type = "old"
                    else:
                        device_type = "swx"

                    if device_type == "":
                        cur.execute("SELECT imei FROM things WHERE imei = %s", (message[1],))
                        # Fetch the result
                        result = cur.fetchone()
                        # Check if a row was returned and if the IMEI matches
                        if result and result[0] == message[1]:
                            device_type = "old"
                        else:
                            device_type = "swx"
                    
                    
                    if device_type != "":

                        if device_type == "old":
                            q = "UPDATE things SET " + topic + "='"+ message[3] +"', lastupdated=NOW() WHERE swd_imei = '"+ message[1] +"'"
                        elif device_type == "swx":
                            q = "UPDATE things SET " + topic + "='"+ message[3] +"', lastupdated=NOW() WHERE imei = '"+ message[1] +"'"
                        
                            
                        if device_type != "" and DEBUG_MODE != "True":
                            cur.execute(q)
                        else:
                            logger.info(q)

                    else:
                        strings_to_check = ["connect", "connection", "disconnect", "location", "mqttstats"]
                        if not any(s in topic for s in strings_to_check):
                            if device_type == "old":
                                q = "INSERT INTO things (swd_imei, " + topic + "," + "lastupdated) VALUES ('" + message[1] +"', '" + message[3] + "', NOW())"
                            elif device_type == "swx":
                                q = "INSERT INTO things (imei, " + topic + "," + "lastupdated) VALUES ('" + message[1] +"', '" + message[3] + "', NOW())"
                            
                            if DEBUG_MODE != "True":
                                cur.execute(q)
                            else:
                                logger.info(q)

                except errors.UndefinedColumn as e:
                        conn.rollback()
                        logger.warning("Column not present: %s", str(e))
                        logger.info("Creating column: ", topic)
                        sql="ALTER TABLE things ADD COLUMN " + topic + " TEXT;"
                        if DEBUG_MODE != "True":
                            
                                cur.execute(sql)
                                conn.commit()
                            
                        else:
                            logger.info(sql)
                        
                except Exception as e:
                    logger.error("Error inserting/updating message in database: %s", str(e))
                    sys.exit(1)
                    # Handle unique constraint violations separately
                    
            conn.commit()
            
        except Exception as e:
            logger.error("Error writing to database: %s", str(e))
            sys.exit(1)
            
def opendatabase():
    global conn
    
    # Retrieve Postgres database credentials from environment variables
    host = os.environ.get('POSTGRES_HOST')
    dbname = os.environ.get('POSTGRES_DBNAME')
    user = os.environ.get('POSTGRES_USER')
    password = os.environ.get('POSTGRES_PASSWORD')
    port = os.environ.get('POSTGRES_PORT')
    
    try:
        logger.info("Opening database")
        conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    except Exception as e:
        logger.critical("Error connecting to database: %s", str(e))
        exit()
    return conn


def send_heartbeat():
    
        # Send a GET request to the heartbeat URL
        response = requests.get(BETTERSTACK_HEARTBEAT_URL)
        
        # Check if the request was successful
        if response.status_code != 200:
            logger.warning(f"Failed to send heartbeat. Status code: {response.status_code}. Will retry.")
    




if __name__ == "__main__":
    
    
    # Open database
    global conn

    

    conn = opendatabase()
    
    # Create a separate thread to process messages in the write queue
    logger.info("Starting message queue thread")
    write_thread = threading.Thread(target=write_to_database, daemon=True)
    write_thread.start()

    # Connect to the MQTT broker
    logger.info("Setting up MQTT")
    cid = os.environ.get('MQTT_CLIENT_ID') + "-" + str(uuid.uuid4().hex)[:8]
    client = mqtt.Client(client_id=cid, clean_session=True)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    send_heartbeat()
    
    
    
    while True:
        try:
            logger.info("Connecting to MQTT broker")
            client.connect(MQTT_HOST, int(MQTT_PORT), 60)
            client.loop_forever()
        except Exception as e:
            logger.critical("Error connecting to MQTT broker: %s", str(e))
            exit()
