from pymongo import MongoClient
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

filter = {"devices": {"$elemMatch": {"deviceCode": "1930217","facility": "SSW","status": "ACTIVE","regester": "unregistered"}}}
newvalues = {"$set": {"devices.$.regester": "registered"}}

try:
    mydb = dbClient[dbName]
    collection = mydb[workertb]
    # Using update_one() method for single updation.
    print("Using update_many")
    result = collection.update_many(filter, newvalues)

    # Printing the updated content of the database
    print("Printing the updated content of the database")
    cursor = collection.find(filter)
    for record in cursor:
        print(record)

except Exception as e:
    print(str(e))



