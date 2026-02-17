#!/usr/bin/python3
# 
# test of attaching to the Effingham sensor feed and inserting data into our supabase table
#

import os
import time
import json

import paho.mqtt.client as mqtt

from pprint import pprint

import psycopg2 ## Connect to PostgreSQL

def get_db_connection():
    # DB_HOST = "localhost"
    # DB_NAME = "postgres-practice"
    # DB_USER = "postgres"
    # DB_PASSWORD = "password"
    # DB_PORT = "3005"

    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_NAME = os.getenv('DB_NAME', 'postgres-practice')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
    DB_PORT = os.getenv('DB_PORT', '3005')

    conn_string = f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}"

    try:
        connection = psycopg2.connect(conn_string)
        print("Connection successful")
        return connection

    except Exception as e:
        print(f"An error occurred: {e}")

def init_db():
    global db_conn
    db_conn = get_db_connection()
    cursor = db_conn.cursor() # Create a cursor object to interact with the database

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS readings (
                   id SERIAL PRIMARY KEY,
                   device_id VARCHAR(255) NOT NULL,
                   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                   ambient_temperature NUMERIC,
                   light_intensity NUMERIC,
                   relative_humidity NUMERIC,
                   soil_temperature NUMERIC,
                   soil_moisture NUMERIC
                   )
    """)
    db_conn.commit() # saving changes
    cursor.close()
    print("Database tables initialized")


def on_connect(mqttc, obj, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print(f"Failed to connect to MQTT, return code {rc}")

def on_message(mqttc, obj, msg):
    global on_mode, off_mode, state, on_modes, off_modes

    # print some debugging info
    #print("topic = ", msg.topic)
    #print("qos = ", str(msg.qos))

    # decode the message payload
    payload = json.loads(msg.payload.decode('UTF-8'))
    #pprint(payload);

    end_device_ids = payload['end_device_ids'];

    device_id = end_device_ids['device_id'];

    print("device_id = ", device_id);

    received_at = payload['received_at'];
    print("received_at = ",received_at);

    uplink_message = payload['uplink_message'];
    #print("uplink_message = ", uplink_message);

    # Our Tektelic sensors send the data we want on port 10

    if 'f_port' in uplink_message and uplink_message['f_port'] == 10 and 'decoded_payload' in uplink_message:

        decoded_payload = uplink_message['decoded_payload'];
        print("decoded_payload = ", decoded_payload);

        version_ids = uplink_message['version_ids'];
        print("version_ids = ", version_ids);

        brand_id = version_ids['brand_id'];

        if brand_id == "tektelic":
            print("### looks like soil moisture, let's put it in the database");
        
            try:
                cursor = db_conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO readings (device_id, ambient_temperature, light_intensity, relative_humidity, soil_temperature, soil_moisture)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (device_id, 
                     decoded_payload['ambient_temperature'], 
                     decoded_payload['light_intensity'], 
                     decoded_payload['relative_humidity'],
                     decoded_payload['Input3_voltage_to_temp'],
                     decoded_payload['watermark1_tension'])
                )
                db_conn.commit()
                cursor.close()

                print("data inserted")
      
            except Exception as e:
                print(f"An error occurred during Supabase insertion: {e}")
            
# end of on_message   

def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_log(mqttc, obj, level, string):
    print(string)

def run_mqtt_listener():
    init_db()
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)

    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_subscribe = on_subscribe
    mqtt_username = os.getenv('MQTT_USERNAME', 'gatech-effingham@ttn')
    mqtt_password = os.getenv('MQTT_PASSWORD', 'your-password')
    mqttc.username_pw_set(mqtt_username, mqtt_password)
    mqttc.connect("nam1.cloud.thethings.network", 1883, 60)
    mqttc.subscribe("v3/+/devices/+/up", 0)

    mqttc.loop_forever()

    
if __name__ == '__main__':
    run_mqtt_listener()