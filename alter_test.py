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



def write_to_database():
    global conn
    
    cur = conn.cursor()
    topic = "TEST"
    sql="ALTER TABLE things ADD COLUMN " + topic + " TEXT;"
    
    cur.execute(sql)
    conn.commit()
                      

            
def opendatabase():
    global conn
    
    # Retrieve Postgres database credentials from environment variables
    host = os.environ.get('POSTGRES_HOST')
    dbname = os.environ.get('POSTGRES_DBNAME')
    user = os.environ.get('POSTGRES_USER')
    password = os.environ.get('POSTGRES_PASSWORD')
    port = os.environ.get('POSTGRES_PORT')
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
  



if __name__ == "__main__":
    
    opendatabase()
    write_to_database()
    exit()    

    