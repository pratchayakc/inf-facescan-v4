from pymongo import MongoClient
from module import alicloudDatabase
import configparser
import os
config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)

infcollection = config_obj["collection"]
infdatabase = config_obj["db"]

dbUser = str(os.environ['DB_USER'])
dbPass = str(os.environ['DB_PASS'])

dbHost = "mongodb://"+infdatabase['nodes']
dbReplicaSet = infdatabase['replicaSet']
dbClient = MongoClient(host=dbHost, replicaset=dbReplicaSet, username=dbUser,
                       password=dbPass, authSource='admin', authMechanism='SCRAM-SHA-256')

dbName = infdatabase['name']
workertb = infcollection['workers']
transectiontb = infcollection['transections']


wmydb = dbClient[dbName]
wmycol = wmydb[workertb]

wmyquery = {"devices": {"$elemMatch": {"facility": "LKB","status": "ACTIVE","regester": "unregistered"}}}
wmydoc = wmycol.find(wmyquery)
count = 0

for x in wmydoc:
    if count < 1 :
        print(x)

        messageId = x['registration']['last_messageId']
        print("message Id : "+messageId)

        myquery = {"messageId": messageId}

        mydb = dbClient[dbName]
        mycol = mydb[transectiontb]
        transection = mycol.find(myquery)

        for t in transection:
            sub_transection = t['transection']
            oper = t['operation']

            for sub_t in sub_transection:
                topic = sub_t['topic']
                mfilter = {"messageId": messageId, "transection": {
                                                "$elemMatch": {"topic": topic, "ackcode": "wating ack"}}}
                newvalues = {"$set": {"transection.$.ackcode": "469", "transection.$.ackdetail": "get URI pic data len too long",
                                    "send_to_WFM": False, "send_to_WFM_timestamp": "", "transection_last_update": ""}}

                mydb = dbClient[dbName]
                collection = mydb[transectiontb]
                # Using update_one() method for single updation.
                print("Using update one")
                result = collection.update_many(mfilter, newvalues)

                # Printing the updated content of the database
                print("Printing the updated content of the database")
                cursor = collection.find(mfilter)
                for record in cursor:
                    print(record)
        
        count = count+1
    else:
        break

print("count : "+str(count))


