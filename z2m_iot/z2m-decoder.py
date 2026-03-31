import requests
from requests.auth import HTTPBasicAuth
import logging
import paho.mqtt.client as mqtt
import ssl
import random
import time
import json
from datetime import datetime

import os
from dotenv import load_dotenv
import logging
import signal
import sys
import threading

#Zigbee2MQTT decoder/encoder for IoT-Open
__version__ = "1.0.0"

load_dotenv()
class Config:
    IOTOPEN_MQTT_HOST = os.getenv("IOTOPEN_MQTT_HOST", "mqtt")
    IOTOPEN_MQTT_PORT = int(os.getenv("IOTOPEN_MQTT_PORT", 1883))
    IOTOPEN_MQTT_USERNAME = os.getenv("IOTOPEN_MQTT_USERNAME")
    IOTOPEN_MQTT_PASSWORD = os.getenv("IOTOPEN_MQTT_PASSWORD")
    IOTOPEN_INSTALLATION_ID = int(os.getenv("IOTOPEN_INSTALLATION_ID",0))
    IOTOPEN_CLIENT_ID = int(os.getenv("IOTOPEN_CLIENT_ID",0))
    IOTOPEN_BASEURL = os.getenv("IOTOPEN_BASEURL")
    MQTT_TOPIC = os.getenv("MQTT_TOPIC", "zigbee2mqtt/#")
    LOCAL_MQTT_HOST = os.getenv("LOCAL_MQTT_HOST", "mqtt")
    LOCAL_MQTT_PORT = int(os.getenv("LOCAL_MQTT_PORT", 1883))



def heartbeat():
    while True:
        logger.info(f"Heartbeat: {sys.argv} is running")
        time.sleep(300)

def handle_signal(sig, frame):
    global running
    logger.info("Shutdown signal received")
    running = False
    sys.exit(0)

def on_connect_local(client, userdata, flags, rc, properties):
    logger.info("Connected to local mqtt")
    client.subscribe("zigbee2mqtt/+", qos=0)
    client.subscribe("zigbee2mqtt/bridge/devices", qos=1)

def on_connect_iot(client, userdata, flags, rc, properties):
    logger.info("Connected to IoT-Open")
    client.subscribe(f"{Config.IOTOPEN_CLIENT_ID}/obj/z2m/+/+/set")


def from_iotopen_to_z2m(client, userdata, msg):
    logger.info(f'Topic_write: {msg.topic}')    
    parts = msg.topic.split("/")

    device = parts[3]
    variable = parts[4]
    action = parts[5]
    logger.info(f'payload:{msg.payload.decode()}')
    new_payload = json.dumps({
        variable: json.loads(msg.payload).get("value")    
    })
    client_z2m.publish(f'zigbee2mqtt/{device}/set',new_payload)

def send_values_to_iotopen(client, userdata, msg):
    z2m_values = json.loads(msg.payload.decode())
    logger.info(f'Device updated: {msg.topic}')    
    for z2m_key, z2m_value in z2m_values.items(): 
        logger.debug(f'{z2m_key}={z2m_value}')
        client_iot.publish(f'{Config.IOTOPEN_CLIENT_ID}/obj/z2m/{msg.topic.split("/")[1]}/{z2m_key}',json.dumps(iot_open_value(z2m_value)))

def check_devices_in_iotopen(client, userdata, msg):
    devices = json.loads(msg.payload)
    coordinators = []
    end_devices = []
    
    for device in devices:
        if device.get("type") == "Coordinator":
            coordinators.append(device)
        elif device.get("type") != "Coordinator":#"EndDevice":
            end_devices.append(device)
            device_id=int(iot_create_device(device))
            #if device_exist!=[]:
            #    device_id=device_exist.get('id')
            #else:
            #    device_id=0
            
            definition = device.get("definition", {})
            exposes = definition.get("exposes", [])

            
            for value in exposes:
                #print(f'{value.get("name")}')
                logger.debug(iot_create_function(value.get("name"), value.get("property"),device.get("ieee_address"),value, device_id))
            
                if "features" in value:
                    for feature in value.get("features", []):
                        if feature.get("property"):
                            logger.debug(iot_create_function(feature.get("name"), feature.get("property"),device.get("ieee_address"),feature, device_id))
           



    logger.info(f'Coordinators:, {len(coordinators)}')
    logger.info(f'End devices:, {len(end_devices)}')

def iot_create_device(device):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {
        "installation_id": Config.IOTOPEN_INSTALLATION_ID,
        "type": 'zigbee',
        "meta": {
            "name": f'{device.get("manufacturer")} - {device.get("definition").get("model")}',
            "ieee_address": f'{device.get("ieee_address")}',
            "manufacturer": f'{device.get("manufacturer")}',
            "model": f'{device.get("definition").get("model")}',
            "model_id": f'{device.get("model_id")}',
            "power_source": f'{device.get("power_source")}',
            "description": f'{device.get("definition").get("description")}'
        }
    }
    iot_devicexists = requests.get(f"{Config.IOTOPEN_BASEURL}/api/v2/devicex/{Config.IOTOPEN_INSTALLATION_ID}?ieee_address={device.get('ieee_address')}", headers=headers, auth=login,)
    #print(iot_devicexists.json()[0].get('id'))
    if iot_devicexists.json()==[]:

        result = requests.post(f"{Config.IOTOPEN_BASEURL}/api/v2/devicex/{Config.IOTOPEN_INSTALLATION_ID}", headers=headers, auth=login, data=json.dumps(payload) )
        return f"{result.json().get('id')}"
        #return result
    else:
        #result = 
        #requests.put(f"{Config.IOTOPEN_BASEURL}/api/v2/devicex/{Config.IOTOPEN_INSTALLATION_ID}/{iot_devicexists.json()[0].get('id')}", headers=headers, auth=login, data=json.dumps(payload) )
        return f"{iot_devicexists.json()[0].get('id')}"


def iot_create_function(name, unit, address, exposed, device_id):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {
        "installation_id": Config.IOTOPEN_INSTALLATION_ID,
        "type": unit,
        "meta": {
            "name": f'{address} - {name}',
            "topic_read": f'obj/z2m/{address}/{name}',
            "device_id": f'{device_id}'
        }
    }
    #Add topic_write if write is enabled
    if exposed.get('access')!=1:
        payload["meta"]['topic_write']=f'obj/z2m/{address}/{name}/set'

    #print(payload)
    iot_functionexists = requests.get(f"{Config.IOTOPEN_BASEURL}/api/v2/functionx/{Config.IOTOPEN_INSTALLATION_ID}?topic_read=obj/z2m/{address}/{name}", headers=headers, auth=login,)

    if iot_functionexists.json()==[]:
        #print('creating')
        result = requests.post(f"{Config.IOTOPEN_BASEURL}/api/v2/functionx/{Config.IOTOPEN_INSTALLATION_ID}", headers=headers, auth=login, data=json.dumps(payload) )
        return result
    else:
        return iot_functionexists
        #print('updating')
        #result = requests.put(f"{Config.IOTOPEN_BASEURL}/api/v2/functionx/{Config.IOTOPEN_INSTALLATION_ID}/{iot_functionexists.json()[0].get('id')}", headers=headers, auth=login, data=json.dumps(payload) )
        #return result

    
def iot_open_value(value, timestamp=None):
    if timestamp is None:
        timestamp = int(time.time())

    if isinstance(value, dict):
        value = value.get("value")

    if value is None:
        return {"timestamp": timestamp, "value": 0, "msg": "null"}

    if isinstance(value, bool):
        return {"timestamp": timestamp, "value": int(value), "msg": str(value)}

    if isinstance(value, str):
        return {"timestamp": timestamp, "value": 0, "msg": value}

    if isinstance(value, (int, float)):
        return {"timestamp": timestamp, "value": value, "msg": ""}

    return {"timestamp": timestamp, "value": 0, "msg": f"unsupported:{type(value).__name__}"}

def main():
    global login, logger, client_iot, client_z2m, client_id, running
    
    logging.Formatter.converter = time.localtime
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger = logging.getLogger(__name__)

    logger.info('Z2M-IOTOPEN converter starting')

    #Only read local .env file for debug. remove when done and use OS environtment inside docker


    login = HTTPBasicAuth(Config.IOTOPEN_MQTT_USERNAME, Config.IOTOPEN_MQTT_PASSWORD)
    # MQTT IoT-Open
    client_id = f'z2m-mqtt-{random.randint(0, 9000)}'
    client_iot = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5, client_id=client_id)
    client_iot.tls_set(certfile=None,
                keyfile=None,
                cert_reqs=ssl.VERIFY_DEFAULT)
    client_iot.reconnect_delay_set(1, 60)
    client_iot.on_connect = on_connect_iot
    client_iot.username_pw_set(Config.IOTOPEN_MQTT_USERNAME, Config.IOTOPEN_MQTT_PASSWORD)
    try:
        client_iot.connect(host=Config.IOTOPEN_MQTT_HOST, port=Config.IOTOPEN_MQTT_PORT, clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, keepalive=120)
    except Exception as e:
        logger.warning(f"Error connecting to MQTT broker for Iot-Open ({Config.IOTOPEN_MQTT_HOST}:{Config.IOTOPEN_MQTT_PORT}): {e}")
        sys.exit(1)



    client_z2m = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5, client_id=client_id)
    client_z2m.reconnect_delay_set(1, 60)
    client_z2m.on_connect = on_connect_local

    try:
        client_z2m.connect(host=Config.LOCAL_MQTT_HOST, port=Config.LOCAL_MQTT_PORT, clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, keepalive=120)
    except Exception as e:
        logger.warning(f"Error connecting to local MQTT broker ({Config.LOCAL_MQTT_HOST}:{Config.LOCAL_MQTT_PORT})for z2m: {e}")
        sys.exit(1)
 
    running = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)


    client_z2m.message_callback_add("zigbee2mqtt/+", send_values_to_iotopen)
    client_z2m.message_callback_add("zigbee2mqtt/bridge/devices", check_devices_in_iotopen)

    client_iot.message_callback_add(f"{Config.IOTOPEN_CLIENT_ID}/obj/z2m/+/+/set", from_iotopen_to_z2m)


    client_z2m.reconnect_delay_set(1, 60)
    #Start heartbeat
    threading.Thread(target=heartbeat, daemon=True).start()

    logger.info('Starting loops')
    client_z2m.loop_start()
    client_iot.loop_start()

    while True:
        time.sleep(0.1)


main()

