from pymongo import MongoClient
from module import alicloudDatabase
from module import alicloudAMQP
import configparser
import os
import string
import random

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)

infcollection = config_obj["collection"]
infdatabase = config_obj["db"]
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

exchange = str(infamqp['exchange'])

def randomString(length):
    letters_and_digits = string.ascii_lowercase + string.digits
    result_str = ''.join(
        (random.choice(letters_and_digits) for i in range(length)))
    #logger.debug("Random alphanumeric String is:", result_str)
    return result_str

dbName = infdatabase['name']
workertb = infcollection['workers']
transectiontb = infcollection['transections']

mydb = dbClient[dbName]
mycol = mydb[workertb]

myquery = {"devices": {"$elemMatch": {"facility": "LKB","status": "ACTIVE","regester": "unregistered"}}}
mydoc = mycol.find(myquery)

count=0

for x in mydoc:
    info = x["info"]

    messageId = randomString(8)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(12)
    
    message = {
        "messageId": messageId,
        "operation": "CREATE_UPDATE_WORKER",
        "info": info
    }
    print(message)

    routingKey = exchange+"."+str(infroute['workersync'])
    queueName = str(infqueue['workersync'])
    isqmqpSuccess = alicloudAMQP.amqpPublish(
        exchange, routingKey, message, queueName)
    print("publish amqp workersync success ? : "+str(isqmqpSuccess))

    count=count+1

print("count : "+str(count))



