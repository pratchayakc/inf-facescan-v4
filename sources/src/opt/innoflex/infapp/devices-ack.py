""" This module receive person ack from device """
""" devices-ack.py """

from module import alicloudDatabase
from module import connection
from pymongo import MongoClient
from datetime import datetime
import configparser
import threading
import logging.handlers
import logging
import pika
import ast
import sys
import os
config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)

infcollection = config_obj["collection"]
infdatabase = config_obj["db"]
inftopic = config_obj["topic"]
infqueue = config_obj["queue"]
inflog = config_obj["log"]
infetc = config_obj["etc"]
infamqp = config_obj["amqp"]
infroute = config_obj["route"]
infoperation = config_obj["operation"]

dbUser = str(os.environ['DB_USER'])
dbPass = str(os.environ['DB_PASS'])

dbHost = "mongodb://"+infdatabase['nodes']
dbReplicaSet = infdatabase['replicaSet']
dbClient = MongoClient(host=dbHost, replicaset=dbReplicaSet, username=dbUser,
                       password=dbPass, authSource='admin', authMechanism='SCRAM-SHA-256')

dbName = infdatabase['name']
workertb = infcollection['workers']
transectiontb = infcollection['transections']

create_worker_operation_name = infoperation['create_worker']

parent_topic = inftopic['parent']
queueName = infqueue['deviceAck']
exchange = infamqp['exchange']
route = str(infroute['deviceack'])
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
            body = body.replace('\\r\\n', '')
            body = body.replace('\\', '')
            body = body[1:]
            body = body[:-1]
            
            message = ast.literal_eval(body)
            operation = message['operator']
            messageId = message['messageId']

            mydb = dbClient[dbName]
            mycol = mydb[transectiontb]

            logname = 'DeviceAck'+' ['+messageId+'] '
            logger = setup_logger(logname, LOG_PATH+"/"+"inf-devices-ack.log")

            logger.debug(body)

            if operation == "DelPerson-Ack":
                
                code = message['code']
                facedevice = str(message['info']['facesluiceId'])
                deviceCode = facedevice.split("@@@")[1]
                workerCode = message['info']['customId']

                if code != "200":
                    code_detail = message['info']['detail']
                else:
                    code_detail = 'No error message'

                logger.debug("operation : "+operation)
                logger.debug("messageId : "+messageId)
                logger.debug("code : "+code)
                logger.debug("facedevice : "+deviceCode)
                logger.debug("workerCode : "+workerCode)

                data = {
                    "messageId": messageId,
                    "deviceCode": deviceCode,
                    "workerCode": workerCode,
                    "code": code,
                    "code_detail": code_detail
                }

                myquery = {"messageId": messageId}
                transection = mycol.find(myquery)

                for t in transection:
                    sub_transection = t['transection']
                    oper = t['operation']

                    for sub_t in sub_transection:
                        topic = sub_t['topic']
                        logger.debug("topic : "+topic)
                        if topic == parent_topic+"/face/"+deviceCode:
                            logger.debug("topic : "+topic)

                            ack_time = datetime.now()
                            ack_stamp = ack_time.strftime("%Y-%m-%d %H:%M:%S")
                            # update on transection table
                            query = {"messageId": messageId, "transection": {
                                "$elemMatch": {"topic": topic, "ackcode": "wating ack"}}}
                            newvalues = {"$set": {
                                "transection.$.ackcode": code, "transection.$.ackdetail": code_detail, "transection.$.receive_ack_time": ack_stamp}}
                            isSuccess = alicloudDatabase.updateOneToDB(
                                transectiontb, query, newvalues)
                            log = {
                                "data": data,
                                "tasks": {
                                    "database": {
                                        "collection": transectiontb,
                                        "operation": "update",
                                        "query": query,
                                        "newvalue": newvalues,
                                        "success": isSuccess
                                    }
                                }
                            }

                            logs = str(log)
                            logger.info(logs)

                            if code == "200" or code == "464":
                                if oper == create_worker_operation_name:
                                    # INACTIVE Worker
                                    # update on workerlist table

                                    ack_time = datetime.now()
                                    ack_stamp = ack_time.strftime("%Y-%m-%d %H:%M:%S")

                                    query = {"registration.last_messageId": messageId, "devices": {
                                        "$elemMatch": {"deviceCode": deviceCode}}}
                                    newvalues = {"$set": {"registration.last_ack_update": ack_stamp,
                                                          "devices.$.regester": "unregistered", "devices.$.last_ack_update": ack_stamp, 
                                                          "devices.$.last_ack_detail": code_detail, "devices.$.last_ack_messageId": messageId}}

                                    isSuccess = alicloudDatabase.updateOneToDB(
                                        workertb, query, newvalues)
                                    log = {
                                        "data": data,
                                        "tasks": {
                                            "database": {
                                                "collection": workertb,
                                                "operation": "update",
                                                "query": query,
                                                "newvalue": newvalues,
                                                "success": isSuccess
                                            }
                                        }
                                    }

                                    logs = str(log)
                                    logger.info(logs)

                channel.basic_ack(delivery_tag=method_frame.delivery_tag)

            elif operation == "EditPerson-Ack":
                messageId = message['messageId']
                code = str(message['code'])
                facedevice = str(message['info']['facesluiceId'])
                deviceCode = facedevice.split("@@@")[1]
                workerCode = message['info']['customId']

                if code != "200":
                    code_detail = message['info']['detail']
                else:
                    code_detail = 'No error message'

                logger.debug("operation : "+operation)
                logger.debug("messageId : "+messageId)
                logger.debug("code : "+code)
                logger.debug("facedevice : "+deviceCode)
                logger.debug("workerCode : "+workerCode)

                data = {
                    "messageId": messageId,
                    "deviceCode": deviceCode,
                    "workerCode": workerCode,
                    "code": code,
                    "code_detail": code_detail
                }

                mydb = dbClient[dbName]
                mycol = mydb[transectiontb]
                myquery = {"messageId": messageId}
                transection = mycol.find(myquery)

                for t in transection:
                    sub_transection = t['transection']
                    oper = t['operation']

                    for sub_t in sub_transection:
                        topic = sub_t['topic']
                        logger.debug("topic : "+topic)
                        if topic == parent_topic+"/face/"+deviceCode:
                            logger.debug("topic : "+topic)

                            ack_time = datetime.now()
                            ack_stamp = ack_time.strftime("%Y-%m-%d %H:%M:%S")

                            # update on transection table
                            query = {"messageId": messageId, "transection": {
                                "$elemMatch": {"topic": topic, "ackcode": "wating ack"}}}
                            newvalues = {"$set": {
                                "transection.$.ackcode": code, "transection.$.ackdetail": code_detail,"transection.$.receive_ack_time": ack_stamp}}
                            isSuccess = alicloudDatabase.updateOneToDB(
                                transectiontb, query, newvalues)
                            log = {
                                "data": data,
                                "tasks": {
                                    "database": {
                                        "collection": transectiontb,
                                        "operation": "update",
                                        "query": query,
                                        "newvalue": newvalues,
                                        "success": isSuccess
                                    }
                                }
                            }

                            logs = str(log)
                            logger.info(logs)

                            if code == "200":
                                if oper == create_worker_operation_name:
                                    # create worker
                                    # update on workerlist table

                                    ack_time = datetime.now()
                                    ack_stamp = ack_time.strftime("%Y-%m-%d %H:%M:%S")

                                    query = {"registration.last_messageId": messageId, "devices": {
                                        "$elemMatch": {"deviceCode": deviceCode}}}
                                    newvalues = {"$set": {"registration.last_ack_update": ack_stamp,
                                                          "devices.$.regester": "registered", "devices.$.last_ack_update": ack_stamp, 
                                                          "devices.$.last_ack_detail": code_detail, "devices.$.last_ack_messageId": messageId}}

                                    isSuccess = alicloudDatabase.updateOneToDB(
                                        workertb, query, newvalues)
                                    log = {
                                        "data": data,
                                        "tasks": {
                                            "database": {
                                                "collection": workertb,
                                                "operation": "update",
                                                "query": query,
                                                "newvalue": newvalues,
                                                "success": isSuccess
                                            }
                                        }
                                    }

                                    logs = str(log)
                                    logger.info(logs)

                channel.basic_ack(delivery_tag=method_frame.delivery_tag)

            else:
                # not DEVICE_ACK package , Do nothing
                logger.debug('not DEVICE_ACK package , Do nothing')
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        except Exception as e:
            logger.error("Error on "+str(e) +
                         ", or Invalid message format -- drop message")
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def run(self):
        logger = setup_logger('DeviceAck', LOG_PATH+"/"+"inf-devices-ack.log")
        try:
            logger.debug('starting thread to consume from rabbit...')
            self.channel.start_consuming()

        except Exception as e:
            logger.error(str(e))


def main():
    logger = setup_logger('DeviceAck', LOG_PATH+"/"+"inf-devices-ack.log")
    for i in range(THREADS):
        logger.debug('launch thread '+str(i))
        td = ThreadedConsumer()
        td.start()


if __name__ == "__main__":
    main()
