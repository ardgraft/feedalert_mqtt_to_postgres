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
        logger.error("Connection failed. Retrying in 5 seconds...")
        

def on_disconnect(client, userdata, rc):
    max_retries = 10       # Maximum number of reconnect attempts
    retry_delay = 5        # Delay between retries in seconds
    retries = 0
    
    if rc != 0:
        logger.warning("MQTT connection lost. Reconnecting...")

    while retries < max_retries:
        logger.warning(f"Attempting to reconnect, try {retries + 1} of {max_retries}...")
        try:
            client.reconnect()
            logger.info("Reconnected successfully.")
            return  # Exit function if reconnected successfully
        except Exception as e:
            retries += 1
            logger.error(f"Reconnect attempt {retries} failed. Error: {str(e)}")
            if retries < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)  # Wait before retrying
            else:
                logger.critical("Max reconnect attempts reached. Exiting script.")
                exit(1)

    logger.critical("Unable to reconnect after multiple attempts. Exiting script.")
    exit(1)

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
        

import sys
import queue
import logging
from psycopg2 import errors

def write_to_database():
    global conn, telit
    
    logger = logging.getLogger(__name__)
    
    try:
        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Continuously process messages in the queue
        while True:
            # Check and send heartbeat if needed
            
            check_and_send_heartbeat()

            try:
                # Try to get a message from the queue without blocking
                message = write_queue.get(block=True)  # Blocks until a message is available
                print(f"Processing message: {str(message)}")

                # Process the message and interact with the database
                process_message(cur, message)

            except queue.Empty:
                # No message in queue, continue looping and waiting for messages
                continue
            except errors.UndefinedColumn as e:
                conn.rollback()
                print("Column not present: %s", str(e))
                print(f"Creating column: {message[2]}")
                create_column_if_missing(cur, message[2])
            except Exception as e:
                conn.rollback()
                print("Error inserting/updating message in database: %s", str(e))
                sys.exit(1)

            # Commit the transaction after processing each message
            conn.commit()

    except Exception as e:
        print("Error writing to database: %s", str(e))
        sys.exit(1)
    finally:
        if cur:
            cur.close()


def process_message(cur, message):
    """Process each message and insert/update the database."""
    env = MQTT_ENV
    last_slash_index = message[2].rfind('/')
    topic = message[2][last_slash_index + 1:]
    imei = message[1]

    # Insert message into the database
    
    cur.execute(
        "INSERT INTO mqtt (timestamp, imei, message, payload, crc, env, topic) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (*message, env, topic)
    )
    
    # Check device type and update information accordingly
    device_type = get_device_type(cur, imei)

    if device_type:
        update_device_info(cur, message, topic, device_type)
    else:
        insert_new_device(cur, message, topic, device_type)


def get_device_type(cur, imei):
    """Checks if the device exists and returns its type."""
    device_type = ""

    # Check if device is 'old'
    cur.execute("SELECT swd_imei FROM things WHERE swd_imei = %s", (imei,))
    result = cur.fetchone()
    if result and result[0] == imei:
        return "old"

    # Check if device is 'swx'
    cur.execute("SELECT imei FROM things WHERE imei = %s", (imei,))
    result = cur.fetchone()
    if result and result[0] == imei:
        return "swx"

    return device_type

def update_device_info(cur, message, topic, device_type):
    """Updates the device information in the database."""
    imei = message[1]
    payload = message[3]

    if device_type == "old":
        query = "UPDATE things SET {} = %s, lastupdated = NOW() WHERE swd_imei = %s".format(topic)
    else:  # device_type == "swx"
        query = "UPDATE things SET {} = %s, lastupdated = NOW() WHERE imei = %s".format(topic)

    if DEBUG_MODE != "True":
        cur.execute(query, (payload, imei))
    else:
        print(query)

def insert_new_device(cur, message, topic, device_type):
    """Inserts a new device into the database."""
    imei = message[1]
    payload = message[3]
    
    strings_to_check = ["connect", "connection", "disconnect", "location", "mqttstats"]
    if not any(s in topic for s in strings_to_check):
        if device_type == "old":
            query = "INSERT INTO things (swd_imei, {}, lastupdated, firstseen) VALUES (%s, %s, NOW(), NOW())".format(topic)
        else:  # device_type == "swx"
            query = "INSERT INTO things (imei, {}, lastupdated, firstseen) VALUES (%s, %s, NOW(), NOW())".format(topic)

            cur.execute(query, (imei, payload))        
            print(query)

def create_column_if_missing(cur, topic):
    """Creates a missing column in the 'things' table."""
    
    sql = f"ALTER TABLE things ADD COLUMN {topic} TEXT;"
    cur.execute(sql)
    conn.commit()
    print(sql)


def opendatabase():
    global conn
    
    # Retrieve Postgres database credentials from environment variables
    host = os.environ.get('POSTGRES_HOST')
    dbname = os.environ.get('POSTGRES_DBNAME')
    user = os.environ.get('POSTGRES_USER')
    password = os.environ.get('POSTGRES_PASSWORD')
    port = os.environ.get('POSTGRES_PORT')
    
    try:
        print("Connecting to database...")
        conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
        # conn.autocommit = True
        logger.info("Connected to database successfully.")
    except Exception as e:
        
        print("Error connecting to database: %s", str(e))
        sys.exit(1)
    return conn


def send_heartbeat():
    
    try:
        # Send a GET request to the heartbeat URL
        response = requests.get(BETTERSTACK_HEARTBEAT_URL)
        
        # Check if the request was successful
        if response.status_code != 200:
            logger.warning(f"Failed to send heartbeat. Status code: {response.status_code}. Will retry.")
    
    except requests.exceptions.RequestException as e:
        # Handle any exceptions that occur during the request
        logger.error(f"Heartbeat, an error occurred: {e}")




if __name__ == "__main__":
    
    # Open database
    global conn
    conn = opendatabase()
    
    write_thread = threading.Thread(target=write_to_database, daemon=True)
    write_thread.start()

    # Connect to the MQTT broker
    logger.info("Setting up MQTT")
    cid = os.environ.get('MQTT_CLIENT_ID') + "-" + str(uuid.uuid4().hex)[:8]
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=cid, clean_session=True)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.connect(MQTT_HOST, int(MQTT_PORT), 60)
    client.loop_forever()
        
       