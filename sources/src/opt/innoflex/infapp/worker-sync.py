""" This module receive create worker message from wfm """
""" worker-sync.py """

from module import alicloudMQTT
from module import connection
from module import alicloudDatabase
from datetime import datetime
from pymongo import MongoClient
import configparser
import threading
import logging
import pika
import ast
import sys
import os

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)

infcollection = config_obj["collection"]
infqueue = config_obj["queue"]
infetc = config_obj["etc"]
inflog = config_obj["log"]
infamqp = config_obj["amqp"]
infroute = config_obj["route"]
infdatabase = config_obj["db"]
inftopic = config_obj["topic"]


dbUser = str(os.environ['DB_USER'])
dbPass = str(os.environ['DB_PASS'])

dbHost = "mongodb://"+infdatabase['nodes']
dbReplicaSet = infdatabase['replicaSet']
dbClient = MongoClient(host=dbHost, replicaset=dbReplicaSet, username=dbUser,
                       password=dbPass, authSource='admin', authMechanism='SCRAM-SHA-256')

dbName = infdatabase['name']
workertb = infcollection['workers']
transectiontb = infcollection['transections']

parent_topic = inftopic['parent']

queueName = infqueue['workersync']
exchange = infamqp['exchange']
route = str(infroute['workersync'])
routing_key = exchange+"."+route

LOG_PATH = inflog['path']
THREADS = int(infetc['threadnum'])

loggers = {}

def setup_logger(name, log_file, level=logging.INFO):
    global loggers

    if loggers.get(name):
        return loggers.get(name)

    else:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        fileFormat = logging.Formatter(
            '{"timestamp":"%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}')
        fileHandler = logging.FileHandler(log_file)
        fileHandler.setFormatter(fileFormat)
        fileHandler.setLevel(level)
        logger.addHandler(fileHandler)

        streamFormat = logging.Formatter(
            '%(asctime)s %(name)s [%(levelname)s] %(message)s')
        streamHandler = logging.StreamHandler(sys.stdout)
        streamHandler.setFormatter(streamFormat)
        streamHandler.setLevel(logging.DEBUG)
        logger.addHandler(streamHandler)

        # reduce pika log level
        logging.getLogger("pika").setLevel(logging.WARNING)
        loggers[name] = logger

    return logger


class ThreadedConsumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        connect = pika.BlockingConnection(connection.getConnectionParam())
        self.channel = connect.channel()
        self.channel.queue_declare(queueName, durable=True, auto_delete=False)
        self.channel.basic_qos(prefetch_count=THREADS*10)
        threading.Thread(target=self.channel.basic_consume(
            queueName, on_message_callback=self.on_message))

    def on_message(self, channel, method_frame, header_frame, body):
        try:
            body = str(body.decode())
            message = ast.literal_eval(body)
            operation = message['operation']
            messageId = message['messageId']

            logname = 'WorkerSync'+' ['+messageId+'] '
            logger = setup_logger(logname, LOG_PATH+"/"+"inf-worker-sync.log")

            logger.debug(body)
            logger.debug("operation : "+operation)

            ####################### update worker database #######################
            if operation == "CREATE_UPDATE_WORKER":
                info = message['info']
                workerCode = message['info']['workerCode']
                workerName = message['info']['name']
                workerGender = message['info']['gender']
                facilities = message['info']['facilities']
                status = message['info']['status']
                pictureURL = message['info']['pictureURL']

                time = datetime.now()
                last_update = time.strftime("%Y-%m-%d %H:%M:%S")

                # find existing workercode
                mydb = dbClient[dbName]
                mycol = mydb[workertb]
                myquery = {"_id": workerCode}
                w_exist = mycol.find(myquery)
                w_count = 0
                for w in w_exist:
                    w_count = w_count+1

                logger.debug("Existing worker : " + str(w_count))

                # Exising worker
                if w_count > 0:
                    logger.debug("worker code is already exist")
                    w_exist = mycol.find(myquery)
                    for w in w_exist:
                        logger.debug(w)
                        # update info section
                        newvalues = {"$set": {"info": info}}
                        isUpdateInfo = alicloudDatabase.updateOneToDB(
                            workertb, myquery, newvalues)
                        logger.debug(
                            "update info section success ? : "+str(isUpdateInfo))

                        # update registration section
                        registration = {
                            "last_messageId": messageId,
                            # receive create/update message timestamp (lastest)
                            "received_time": last_update,
                            "last_ack_update": ""  # receive lastest ack message timestamp
                        }
                        newvalues = {"$set": {"registration": registration}}
                        isUpdateRegis = alicloudDatabase.updateOneToDB(
                            workertb, myquery, newvalues)
                        logger.debug(
                            "update registration section success ? : "+str(isUpdateRegis))

                        # update devices section
                        logger.debug("--- update devices section ")
                        old_devices = w["devices"]

                        # list exist facilities
                        logger.debug("--- list exist facilities ")
                        old_faci = []
                        for ex in old_devices:
                            facility = ex["facility"]
                            old_faci.append(facility)

                        old_faci = list(dict.fromkeys(old_faci))
                        logger.debug("old_faci : "+str(old_faci))

                        new_faci = facilities
                        logger.debug("new_faci : "+str(new_faci))

                        add_faci = list(set(new_faci) - set(old_faci))  # add
                        del_faci = list(set(old_faci) - set(new_faci))  # del

                        logger.debug("Add new facility : "+str(add_faci))
                        logger.debug("Del old facility : "+str(del_faci))

                        # update new devices
                        all_devices = old_devices
                        if len(add_faci) > 0:
                            devices = alicloudDatabase.getAlldevicesByfacility(
                                add_faci)
                            for d in devices:
                                device = d
                                del device["_id"]
                                del device["name"]
                                del device["ipaddr"]

                                device["status"] = status
                                device["regester"] = "unregistered"
                                device["last_ack_messageId"] = messageId
                                device["last_ack_detail"] = ""
                                device["last_ack_update"] = ""

                                all_devices.append(device)

                            logger.debug("--- all devices ---")
                            logger.debug(all_devices)
                            newvalues = {"$set": {"devices": all_devices}}
                            isUpdateDevices = alicloudDatabase.updateOneToDB(
                                workertb, myquery, newvalues)
                            logger.debug(
                                "update new devices success ? : "+str(isUpdateDevices))
                        else:
                            isUpdateDevices = False
                            logger.debug(
                                "update new devices success ? : "+str(isUpdateDevices))

                        # update old devices with new status
                        if len(old_faci) > 0:
                            devices = alicloudDatabase.getAlldevicesByfacility(
                                old_faci)
                            for d in devices:
                                deviceCode = d['deviceCode']
                                logger.debug("device code : "+str(deviceCode))
                                query = {"info.workerCode": workerCode, "devices": {
                                    "$elemMatch": {"deviceCode": deviceCode}}}

                                if status == "BLACKLISTED":
                                    logger.debug("query : "+str(query))
                                    newvalues = {"$set": {
                                        "devices.$.status": "BLACKLISTED", "devices.$.regester": "unregistered", "devices.$.last_ack_messageId": messageId, "devices.$.last_ack_detail": "", "devices.$.last_ack_update": ""}}
                                elif status == "ACTIVE":
                                    logger.debug("query : "+str(query))
                                    newvalues = {"$set": {
                                        "devices.$.status": "ACTIVE", "devices.$.regester": "unregistered", "devices.$.last_ack_messageId": messageId, "devices.$.last_ack_detail": "", "devices.$.last_ack_update": ""}}
                                elif status == "INACTIVE":
                                    logger.debug("query : "+str(query))
                                    newvalues = {"$set": {
                                        "devices.$.status": "INACTIVE", "devices.$.regester": "registered", "devices.$.last_ack_messageId": messageId, "devices.$.last_ack_detail": "", "devices.$.last_ack_update": ""}}
                                else:
                                    logger.debug(
                                        "Worker status is not ACTIVE, INACTIVE or BLACKLISTED")

                                logger.debug("new value : "+str(newvalues))
                                isUpdateDevices = alicloudDatabase.updateOneToDB(
                                    workertb, query, newvalues)
                                logger.debug(
                                    "update old devices with new status success ? : "+str(isUpdateDevices))
                        else:
                            isUpdateDevices = False
                            logger.debug(
                                "update old devices with new status success ? : "+str(isUpdateDevices))

                        # update del devices with inactive status
                        if len(del_faci) > 0:
                            devices = alicloudDatabase.getAlldevicesByfacility(
                                del_faci)
                            for d in devices:
                                deviceCode = d['deviceCode']
                                logger.debug("device code : "+str(deviceCode))
                                query = {"info.workerCode": workerCode, "devices": {
                                    "$elemMatch": {"deviceCode": deviceCode}}}
                                logger.debug("query : "+str(query))
                                newvalues = {"$set": {
                                    "devices.$.status": "INACTIVE", "devices.$.regester": "registered", "devices.$.last_ack_messageId": messageId, "devices.$.last_ack_detail": "", "devices.$.last_ack_update": ""}}
                                logger.debug("new value : "+str(newvalues))
                                isUpdateDevices = alicloudDatabase.updateOneToDB(
                                    workertb, query, newvalues)
                                logger.debug(
                                    "update del devices with inactive status success ? : "+str(isUpdateDevices))
                        else:
                            isUpdateDevices = False
                            logger.debug(
                                "update del devices with inactive status success ? : "+str(isUpdateDevices))

                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                # new worker
                else:
                    # registration section
                    registration = {
                        "last_messageId": messageId,
                        # receive create/update message timestamp (lastest)
                        "received_time": last_update,
                        "last_ack_update": ""  # receive lastest ack message timestamp
                    }

                    # devices section
                    all_devices = []
                    devices = alicloudDatabase.getAlldevicesByfacility(
                        facilities)
                    for d in devices:
                        device = d
                        del device["_id"]
                        del device["name"]
                        del device["ipaddr"]
                        # logger.debug(d)
                        device["status"] = status
                        device["regester"] = "unregistered"
                        device["last_ack_messageId"] = ""
                        device["last_ack_detail"] = ""
                        device["last_ack_update"] = ""

                        all_devices.append(device)
                        logger.debug(all_devices)

                    del message["messageId"]
                    del message["operation"]

                    message["registration"] = registration
                    message["devices"] = all_devices
                    message["_id"] = workerCode

                    logger.debug(message)
                    isSuccess = alicloudDatabase.insertToDB(workertb, message)
                    logger.debug(
                        "insert data to database success ? : "+str(isSuccess))

                    if isSuccess == False:
                        channel.basic_reject(
                            delivery_tag=method_frame.delivery_tag)
                    else:
                        channel.basic_ack(
                            delivery_tag=method_frame.delivery_tag)

                ###################################  Check unregistered worker / Create transection / Sent mqtt message to devices ########################
                wkquery = {"_id": workerCode}
                workerlist = mycol.find(wkquery, no_cursor_timeout=True)

                for w in workerlist:
                    devices = w["devices"]
                    all_transection = []
                    all_mqtt_publish = []

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

            else:
                # not CREATE_UPDATE_WORKER package , Do nothing
                logger.debug('not CREATE_UPDATE_WORKER package , Do nothing')
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        except Exception as e:
            logger.error("Error on "+str(e) +
                         ", or Invalid message format -- drop message")
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def run(self):
        logger = setup_logger('WorkerSync', LOG_PATH+"/"+"inf-worker-sync.log")
        try:
            logger.debug('starting thread to consume from AMQP...')
            self.channel.start_consuming()

        except Exception as e:
            logger.error(str(e))


def main():
    logger = setup_logger('WorkerSync', LOG_PATH+"/"+"inf-worker-sync.log")
    for i in range(THREADS):
        logger.debug('launch thread '+str(i))
        td = ThreadedConsumer()
        td.start()


if __name__ == "__main__":
    main()
