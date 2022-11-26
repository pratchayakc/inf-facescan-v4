""" This module recieve devices basic message from MQTT broker and forward to AMQP broker """
""" restart.py """


from paho.mqtt.client import MQTT_LOG_INFO, MQTT_LOG_NOTICE, MQTT_LOG_WARNING, MQTT_LOG_ERR, MQTT_LOG_DEBUG
from paho.mqtt import client as mqtt
from module import alicloudMQTT
import configparser
import ast
import string
import random

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
inftopic = config_obj["topic"]
infmqtt = config_obj["mqtt"]

parent_topic = str(inftopic['parent'])

def randomString(length):
    letters_and_digits = string.ascii_lowercase + string.digits
    result_str = ''.join(
        (random.choice(letters_and_digits) for i in range(length)))
    #logger.debug("Random alphanumeric String is:", result_str)
    return result_str

messageId = randomString(8)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(12)
print("messageId : "+messageId)

msg_mqtt = {}
msg_mqtt["message"] = {
"operator": "RebootDevice",
"messageId": messageId,
"info":
    {   
    }
}
all_mqtt_publish = []
pub_topic = parent_topic +"/face/1930214"

msg_mqtt["topic"] = pub_topic

all_mqtt_publish.append(msg_mqtt)
print("---- Publish MQTT  ----")
result = alicloudMQTT.mqttPublish(all_mqtt_publish)
for i in result:
    print(i)