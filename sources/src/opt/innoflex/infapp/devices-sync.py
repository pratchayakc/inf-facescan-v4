""" This module recieve devices basic message from MQTT broker and forward to AMQP broker """
""" device-sync.py """


from paho.mqtt.client import MQTT_LOG_INFO, MQTT_LOG_NOTICE, MQTT_LOG_WARNING, MQTT_LOG_ERR, MQTT_LOG_DEBUG
from paho.mqtt import client as mqtt
from module import alicloudDatabase
from module import alicloudMQTT
from pymongo import MongoClient
from datetime import datetime
import configparser
import logging
import socket
import sys
import ssl
import os
import ast
import string
import random

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
inftopic = config_obj["topic"]
infmqtt = config_obj["mqtt"]
inflog = config_obj["log"]

groupId = infmqtt['groupid']
brokerUrl = infmqtt['endpoint']

username = str(os.environ['BASIC_USER'])
password = str(os.environ['BASIC_PASS'])

parent_topic = str(inftopic['parent'])
topic = parent_topic+"/face/basic"

hostname = socket.gethostname()

client_id = groupId+'@@@'+hostname+"-basic"
LOG_PATH = inflog['path']

### Database ###
infcollection = config_obj["collection"]
infdatabase = config_obj["db"]

dbUser = str(os.environ['DB_USER'])
dbPass = str(os.environ['DB_PASS'])

dbHost = "mongodb://"+infdatabase['nodes']
dbReplicaSet = infdatabase['replicaSet']
dbClient = MongoClient(host=dbHost, replicaset=dbReplicaSet, username=dbUser,
                       password=dbPass, authSource='admin', authMechanism='SCRAM-SHA-256')

dbName = infdatabase['name']
devicetb = infcollection['devices']
workertb = infcollection['workers']
transectiontb = infcollection['transections']

parent_topic = inftopic['parent']

mydb = dbClient[dbName]
mywk_col = mydb[workertb]


logger = logging.getLogger('Forwarder-basic')
logger.setLevel(logging.DEBUG)

fileFormat = logging.Formatter(
    '{"timestamp":"%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}')
fileHandler = logging.FileHandler(LOG_PATH+"/inf-device-sync.log")
fileHandler.setFormatter(fileFormat)
fileHandler.setLevel(logging.DEBUG)
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

def randomString(length):
    letters_and_digits = string.ascii_lowercase + string.digits
    result_str = ''.join(
        (random.choice(letters_and_digits) for i in range(length)))
    #logger.debug("Random alphanumeric String is:", result_str)
    return result_str

def deviceSync(cmd, deviceCode, facility, direction):
    count = 0
    myquery = {"info.facilities": facility}
    mydoc = mywk_col.find(myquery)

    for x in mydoc:
        workerCode = x["info"]["workerCode"]
        devices = x["devices"]
        d_exist = False

        if cmd == 'add':
            status = str(devices[0]["status"])
            regester = "unregistered"

            logger.debug(workerCode)
            logger.debug(status)
            logger.debug(regester)

            for de in devices:
                d_code = str(de['deviceCode'])
                if d_code == deviceCode:
                    d_exist = True

            if d_exist == False:  # devices not exist
                logger.debug("device not exist")
                new_device = {"deviceCode": deviceCode, "facility": facility, "type": direction, "status": status,
                              "regester": regester, "last_ack_messageId": "", "last_ack_detail": "", "last_ack_update": ""}
                devices.append(new_device)
                for d in devices:
                    logger.debug(d)

                # update database
                update_query = {"_id": workerCode}
                newvalues = {"$set": {"devices": devices}}
                isUpdateDevices = alicloudDatabase.updateOneToDB(
                    workertb, update_query, newvalues)

                logger.debug("isUpdateDevices : "+str(isUpdateDevices))
                count = count+1

                mydb = dbClient[dbName]
                mycol = mydb[workertb]
                wkquery = {"_id": workerCode}
                workerlist = mycol.find(wkquery, no_cursor_timeout=True)

                logger.debug("--- prepare data for publish mqtt update worker ---")
                for w in workerlist:
                    devices = w["devices"]
                    workerGender = w['info']['gender']
                    workerName = w['info']['name']
                    pictureURL = w['info']['pictureURL']
                    all_transection = []
                    all_mqtt_publish = []

                    messageId = randomString(8)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(12)
                    logger.debug("messageId : "+messageId)

                    for device in devices:
                        d_status = device["status"]
                        d_regis = device["regester"]
                        

                        if d_status == "ACTIVE" and d_regis == "unregistered":
                            tempCardType = 0  # permanent

                            if workerGender == "MALE":
                                gender = 0  # male
                            else:
                                gender = 1  # female

                            worker_json = {
                                "messageId": messageId,
                                "operator": "EditPerson",
                                "info":
                                {
                                    "customId": workerCode,
                                    "name": workerName,
                                    "gender": gender,
                                    "address": device["facility"],
                                    "idCard": workerCode,  # ID number show on web service
                                    "tempCardType": tempCardType,
                                    "personType": 0,  # 0=White list, 1=blacklist
                                    "cardType": 0,
                                    "picURI": pictureURL
                                }
                            }

                            msg_mqtt = {}
                            msg_mqtt["message"] = worker_json
                            pub_topic = parent_topic + \
                                "/face/"+device["deviceCode"]
                            msg_mqtt["topic"] = pub_topic

                            all_mqtt_publish.append(msg_mqtt)

                            ts_time = datetime.now()
                            ts_stamp = ts_time.strftime("%Y-%m-%d %H:%M:%S")

                            transection = {}
                            transection["topic"] = pub_topic
                            transection["body"] = worker_json
                            transection["publish_msg_time"] = ts_stamp
                            transection["receive_ack_time"] = ""
                            transection["ackcode"] = "wating ack"
                            transection["ackdetail"] = ""

                            all_transection.append(transection)

                        elif d_status == "INACTIVE" and d_regis == "registered":
                            worker_json = {
                                "operator": "DelPerson",
                                "messageId": messageId,
                                "info":
                                    {
                                        "customId": workerCode
                                    }
                            }

                            msg_mqtt = {}
                            msg_mqtt["message"] = worker_json
                            pub_topic = parent_topic + \
                                "/face/"+device["deviceCode"]
                            msg_mqtt["topic"] = pub_topic

                            all_mqtt_publish.append(msg_mqtt)

                            ts_time = datetime.now()
                            ts_stamp = ts_time.strftime("%Y-%m-%d %H:%M:%S")

                            transection = {}
                            transection["topic"] = pub_topic
                            transection["body"] = worker_json
                            transection["publish_msg_time"] = ts_stamp
                            transection["receive_ack_time"] = ""
                            transection["ackcode"] = "wating ack"
                            transection["ackdetail"] = ""

                            all_transection.append(transection)

                        elif d_status == "BLACKLISTED" and d_regis == "unregistered":

                            tempCardType = 0  # permanent

                            if workerGender == "MALE":
                                gender = 0  # male
                            else:
                                gender = 1  # female

                            worker_json = {
                                "messageId": messageId,
                                "operator": "EditPerson",
                                "info":
                                {
                                    "customId": workerCode,
                                    "name": workerName,
                                    "gender": gender,
                                    "address": device["facility"],
                                    "idCard": workerCode,  # ID number show on web service
                                    "tempCardType": tempCardType,
                                    "personType": 1,  # 0=Whitelist, 1=blacklist
                                    "cardType": 0,
                                    "picURI": pictureURL
                                }
                            }

                            msg_mqtt = {}
                            msg_mqtt["message"] = worker_json
                            pub_topic = parent_topic + \
                                "/face/"+device["deviceCode"]
                            msg_mqtt["topic"] = pub_topic

                            all_mqtt_publish.append(msg_mqtt)

                            ts_time = datetime.now()
                            ts_stamp = ts_time.strftime("%Y-%m-%d %H:%M:%S")

                            transection = {}
                            transection["topic"] = pub_topic
                            transection["body"] = worker_json
                            transection["publish_msg_time"] = ts_stamp
                            transection["receive_ack_time"] = ""
                            transection["ackcode"] = "wating ack"
                            transection["ackdetail"] = ""

                            all_transection.append(transection)

                    all_ts_time = datetime.now()
                    all_ts_stamp = all_ts_time.strftime("%Y-%m-%d %H:%M:%S")
                    data = {
                        "_id": messageId,
                        "messageId": messageId,
                        "operation": "CREATE_UPDATE_WORKER",
                        "info": w['info'],
                        "transection": all_transection,
                        "transection_create": all_ts_stamp,
                        "transection_last_update": all_ts_stamp,
                        "worker_sync_retries": 0,
                        "send_to_WFM": False,
                        "send_to_WFM_timestamp": ""
                    }

                    isSuccess = alicloudDatabase.insertToDB(
                        transectiontb, data)

                    if isSuccess == True:
                        logger.debug("---- Insert transection success ----")
                        log = {
                            "data": data,
                            "tasks": {
                                "database": {
                                    "collection": workertb,
                                    "operation": "insert",
                                    "success": isSuccess
                                }
                            }
                        }

                        logs = str(log)
                        logger.info(logs.replace("'", '"'))
                    else:
                        logger.warning("---- Message ID already exist, update transection ----")

                        query = {"_id": messageId}
                        logger.debug("query : "+str(query))
                        newvalues = {"$set": {
                            "info": w['info'],
                            "transection": all_transection,
                            "transection_create": all_ts_stamp,
                            "transection_last_update": all_ts_stamp,
                            "worker_sync_retries": 0,
                            "send_to_WFM": False,
                            "send_to_WFM_timestamp": ""}}
                        logger.debug("new value : "+str(newvalues))
                        isUpdateDevices = alicloudDatabase.updateOneToDB(
                                    transectiontb, query, newvalues)

                    # Publish mqtt
                    if len(all_mqtt_publish) > 0 :
                        logger.debug("---- Publish MQTT  ----")
                        result = alicloudMQTT.mqttPublish(all_mqtt_publish)
                        for i in result:
                            logger.debug(i)
                    else :
                        logger.debug("---- No Message for Publish MQTT  ----")

        elif cmd == 'delete':
            # using list comprehension
            # to delete dictionary in list

            res = [i for i in devices if not (i['deviceCode'] == deviceCode)]

            # logger.debuging result
            for r in res:
                logger.debug(r)

            # update database

            update_query = {"_id": workerCode}
            newvalues = {"$set": {"devices": res}}
            isUpdateDevices = alicloudDatabase.updateOneToDB(
                workertb, update_query, newvalues)

            logger.debug("isUpdateDevices : "+str(isUpdateDevices))
            count = count+1

    logger.debug("Total : "+str(count))


def on_message(client, userdata, message):
    try:
        msg = str(message.payload.decode("utf-8"))
        logger.debug(msg)

        message = ast.literal_eval(msg)
        operator = str(message['operator'])
        facedevice = message["info"]["facesluiceId"]
        deviceCode = str(facedevice.split("@@@")[1])
        logger.debug("operator : "+operator)
        logger.debug("deviceCode : "+deviceCode)

        if operator == "Online":
            device_col = mydb["devices"]
            exists = device_col.count_documents({"deviceCode": deviceCode}) > 0
            logger.debug(deviceCode+' is existing >> '+str(exists))

            if exists == False:

                ipaddr = str(message["info"]["ip"])
                deviceName = message["info"]["facesname"]
                facility = deviceName.split("_")[0]
                direction = deviceName.split("_")[1]

                logger.debug("ipaddr : "+ipaddr)
                logger.debug("deviceName : "+deviceName)
                logger.debug("facility : "+facility)
                logger.debug("direction : "+direction)

                device = {"deviceCode": deviceCode, "name": deviceName,
                          "ipaddr": ipaddr, "facility": facility, "type": direction}

                isUpdateDevices = alicloudDatabase.insertToDB(devicetb, device)

                logger.debug("isUpdateDevices : "+str(isUpdateDevices))

                deviceSync('add', deviceCode, facility, direction)

        elif operator == "Offline":
            myquery = {"deviceCode": deviceCode}
            mycol = mydb["devices"]

            for x in mycol.find(myquery):
                facility = x['facility']
                direction = x['type']

            logger.debug("deviceCode : "+deviceCode)
            logger.debug("facility : "+facility)
            logger.debug("direction : "+direction)

            device_col = mydb["devices"]
            x = device_col.delete_many(myquery)
            logger.debug(x.deleted_count, " documents deleted.")

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
