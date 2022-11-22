""" This module check complete transection and send res message to wfm """
""" check-transection.py """

from module import alicloudDatabase
from module import alicloudAMQP
from pymongo import MongoClient
from datetime import datetime
import configparser
import threading
import logging
import socket
import sys
import os
config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)

inftransection = config_obj["transection"]
infcollection = config_obj["collection"]
infoperation = config_obj["operation"]
infdatabase = config_obj["db"]
inftopic = config_obj["topic"]
inflog = config_obj["log"]
infamqp = config_obj["amqp"]
infroute = config_obj["route"]
infqueue = config_obj["queue"]

dbUser = str(os.environ['DB_USER'])
dbPass = str(os.environ['DB_PASS'])

dbHost = "mongodb://"+infdatabase['nodes']
dbReplicaSet = infdatabase['replicaSet']
dbClient = MongoClient(host=dbHost, replicaset=dbReplicaSet, username=dbUser,
                       password=dbPass, authSource='admin', authMechanism='SCRAM-SHA-256')

dbName = infdatabase['name']
workertb = infcollection['workers']
transectiontb = infcollection['transections']

retry_transection_interval = int(inftransection['retry_transection_interval'])
max_retry = int(inftransection['max_transection_retry'])

create_worker_operation_name = infoperation['create_worker']
change_status_operation_name = infoperation['change_status']

appname = socket.gethostname()+'_checkTransction'

parent_topic = inftopic['parent']
sub_topic = inftopic['workerSyncRes']
pub_topic = parent_topic+"/"+sub_topic

exchange = str(infamqp['exchange'])

LOG_PATH = inflog['path']

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


def checkTransectionAndRetry():
    try:
        threading.Timer(retry_transection_interval,
                        checkTransectionAndRetry).start()

        mydb = dbClient[dbName]
        mycol = mydb[transectiontb]

        myquery = {"worker_sync_retries": {
            "$lt": max_retry}, "send_to_WFM": False}
        transections = mycol.find(myquery, no_cursor_timeout=True)

        for t in transections:
            messageId = t['messageId']
            sub_transection = t['transection']
            worker_sync_retries = int(t['worker_sync_retries'])

            logname = 'CheckTransection'+' ['+messageId+'] '
            logger = setup_logger(logname, LOG_PATH+"/"+"inf-transection.log")

            ackcode = ""
            all_ackdetail = []
            deviceFailed = 0
            all_devices = 0
            isFailed = False

            for sub_t in sub_transection:
                all_devices = all_devices+1
                topic = str(sub_t['topic'])
                device = topic.rsplit(',', 1)[-1]

                ackcode = sub_t['ackcode']
                if ackcode == "200":
                    isFailed = False

                elif ackcode == "wating ack":
                    ackcode = 404
                    ackdetail = "no response"
                    detail = str(device)+": '"+str(ackdetail)+"'"
                    all_ackdetail.append(detail)
                    deviceFailed = deviceFailed+1
                    isFailed = True

                else:
                    ackdetail = sub_t['ackdetail']
                    detail = str(device)+": '"+str(ackdetail)+"'"
                    all_ackdetail.append(detail)
                    deviceFailed = deviceFailed+1
                    isFailed = True

            if isFailed == False:
                errMsg = "No error message"
                message = {
                    "messageId": messageId,
                    "operation": create_worker_operation_name+"_RES",
                    "code": 200,
                    "errorMsg": "No error message",
                    "info": t['info']
                }

                logger.info(message)

                routingKey = exchange+"."+str(infroute['workersyncres'])
                queueName = str(infqueue['workersyncres'])
                isqmqpSuccess = alicloudAMQP.amqpPublish(
                    exchange, routingKey, message, queueName)
                logger.info("publish amqp success ? : "+str(isqmqpSuccess))

                all_ts_time = datetime.now()
                all_ts_stamp = all_ts_time.strftime("%Y-%m-%d %H:%M:%S")

                # update on transection table
                query = {"messageId": messageId}
                newvalues = {"$set": {
                    "transection.$.send_to_WFM": isqmqpSuccess, "transection.$.send_to_WFM_timestamp": all_ts_stamp, "transection.$.transection_last_update": all_ts_stamp}}
                isSuccess = alicloudDatabase.updateOneToDB(
                    transectiontb, query, newvalues)

                logger.info(
                    "update on transection table success ? : "+str(isSuccess))

            else:
                worker_sync_retries = worker_sync_retries+1

                all_ts_time = datetime.now()
                all_ts_stamp = all_ts_time.strftime("%Y-%m-%d %H:%M:%S")

                # update on transection table
                query = {"messageId": messageId}
                newvalues = {"$set": {
                    "worker_sync_retries": worker_sync_retries, "transection.$.transection_last_update": all_ts_stamp}}
                isSuccess = alicloudDatabase.updateOneToDB(
                    transectiontb, query, newvalues)

                logger.info(
                    "update on transection table success ? : "+str(isSuccess))

    except Exception as e:
        logger.error(str(e))


checkTransectionAndRetry()
