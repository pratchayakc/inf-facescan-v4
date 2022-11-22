""" This module recieve heartbeat message from MQTT broker and forward to AMQP broker """
""" forwarder-heartbeat.py """

from paho.mqtt.client import MQTT_LOG_INFO, MQTT_LOG_NOTICE, MQTT_LOG_WARNING, MQTT_LOG_ERR, MQTT_LOG_DEBUG
from paho.mqtt import client as mqtt
from module import alicloudAMQP
import configparser
import logging
import socket
import sys
import ssl
import os
config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
inftopic = config_obj["topic"]
infqueue = config_obj["queue"]
infmqtt = config_obj["mqtt"]
infamqp = config_obj["amqp"]
inflog = config_obj["log"]

groupId = infmqtt['groupid']
brokerUrl = infmqtt['endpoint']

exchange = str(infamqp['exchange'])
parent_topic = str(inftopic['parent'])

username = str(os.environ['HBT_USER'])
password = str(os.environ['HBT_PASS'])

topic = parent_topic+"/face/heartbeat"
hbQueue = infqueue['devicehb']

hostname = socket.gethostname()

client_id = groupId+'@@@'+hostname+"-heartbeat"
LOG_PATH = inflog['path']

logger = logging.getLogger('Forwarder-hb')
logger.setLevel(logging.DEBUG)

fileFormat = logging.Formatter(
    '{"timestamp":"%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}')
fileHandler = logging.FileHandler(LOG_PATH+"/inf-forwarder-hb.log")
fileHandler.setFormatter(fileFormat)
fileHandler.setLevel(logging.INFO)
logger.addHandler(fileHandler)

streamFormat = logging.Formatter(
    '%(asctime)s %(name)s [%(levelname)s] %(message)s')
streamHandler = logging.StreamHandler(sys.stdout)
streamHandler.setFormatter(streamFormat)
streamHandler.setLevel(logging.DEBUG)
logger.addHandler(streamHandler)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

def on_log(client, userdata, level, buf):
    try:
        if level == MQTT_LOG_INFO:
            head = 'INFO'
        elif level == MQTT_LOG_NOTICE:
            head = 'NOTICE'
        elif level == MQTT_LOG_WARNING:
            head = 'WARN'
        elif level == MQTT_LOG_ERR:
            head = 'ERR'
        elif level == MQTT_LOG_DEBUG:
            head = 'DEBUG'
        else:
            head = level
        logger.debug('%s: %s' % (head, buf))

    except Exception as e:
        logger.error(str(e))


def on_connect(client, userdata, flags, rc):
    try:
        logger.debug('Connected with result code ' + str(rc))
        client.subscribe(topic, 1)
        msg = "Connected flags" + \
            str(flags)+"result code "+str(rc)+"client1_id  "+str(client)
        logger.debug(msg)
    except Exception as e:
        logger.error(str(e))


def on_message(client, userdata, message):
    try:
        msg = str(message.payload.decode("utf-8"))
        logger.debug(msg)

        routingKey = exchange+".face.heartbeat"
        issuccess = alicloudAMQP.amqpPublish(
            exchange, routingKey, msg, hbQueue)
        logger.debug(issuccess)

    except Exception as e:
        logger.error(str(e))


def on_disconnect(client, userdata, rc):
    try:
        if rc != 0:
            logger.debug('Unexpected disconnection %s' % rc)
    except Exception as e:
        logger.error(str(e))


if __name__ == "__main__":
    try:
        client = mqtt.Client(
            client_id, protocol=mqtt.MQTTv311, clean_session=False)
        client.on_log = on_log
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect

        client.username_pw_set(username, password)
        client.tls_set(ca_certs=None, certfile=None, keyfile=None,
                       cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS, ciphers=None)
        client.connect(brokerUrl, 8883, 60)
        client.loop_forever()

    except Exception as e:
        if str(e) == "[Errno -2] Name or service not known":
            logger.error("Can't connect with "+brokerUrl +
                         " , please check endpoint name in config file.")
        else:
            logger.error(str(e))
