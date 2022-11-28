from module import alicloudDatabase
from pymongo import MongoClient
import configparser
import os
import string
import random

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

def randomString(length):
    letters_and_digits = string.ascii_lowercase + string.digits
    result_str = ''.join(
        (random.choice(letters_and_digits) for i in range(length)))
    #logger.debug("Random alphanumeric String is:", result_str)
    return result_str

add_faci=['LKB']
all_devices = []

wk = [
    '1000490989',
'1000557445',
'1000436249',
'1000436269',
'1000500284',
'1000436125',
'1000436270',
'1000475493',
'1000510240',
'1000508883',
'1000487311',
'1000438303']

for w in wk : 
    all_devices = []
    messageId = randomString(8)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(12)

    workerCode = w
    messageId = messageId
    myquery = {"_id": workerCode}

    devices = alicloudDatabase.getAlldevicesByfacility(add_faci)
    for d in devices:
        device = d
        del device["_id"]
        del device["name"]
        del device["ipaddr"]

        device["status"] = 'ACTIVE'
        device["regester"] = "unregistered"
        device["last_ack_messageId"] = messageId
        device["last_ack_detail"] = ""
        device["last_ack_update"] = ""

        all_devices.append(device)

    print("--- all devices ---")
    print(all_devices)
    newvalues = {"$set": {"devices": all_devices}}
    isUpdateDevices = alicloudDatabase.updateOneToDB(
        workertb, myquery, newvalues)
    print(
        "update new devices success ? : "+str(isUpdateDevices))
