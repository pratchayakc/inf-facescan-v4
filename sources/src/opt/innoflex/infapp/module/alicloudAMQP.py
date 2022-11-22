from module import connection
import logging
import json
import pika
import sys
import string
import random

# Creating and Configuring Logger
logger = logging.getLogger('AMQP-Handler')
streamFormat = logging.Formatter(
    '%(asctime)s %(name)s [%(levelname)s] %(message)s')
streamHandler = logging.StreamHandler(sys.stdout)
streamHandler.setFormatter(streamFormat)
streamHandler.setLevel(logging.DEBUG)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)


def randomString(length):
    letters_and_digits = string.ascii_lowercase + string.digits
    result_str = ''.join(
        (random.choice(letters_and_digits) for i in range(length)))
    #logger.debug("Random alphanumeric String is:", result_str)
    return result_str

def on_message(channel, method_frame, header_frame, body):
    # logger.debug(method_frame.delivery_tag)
    logger.debug(body)
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


def amqpPublish(exchangeName, routingKey, message, queueName):
    try:
        
        print("Connecting AMQP Broker...")
        # connect
        connect = pika.BlockingConnection(connection.getConnectionParam())
        channel = connect.channel()
        channel.basic_qos(prefetch_count=1)
        channel.queue_declare(queueName, durable=True, auto_delete=False)

        try: 
            print("messageId from message")
            messageId = message["messageId"]
            print("messageId : "+messageId)
        except Exception as e:
            print("generate new messageId")
            messageId = randomString(
                    8)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(12)
            print("messageId : "+messageId)

        # send message
        print("Publishing Message...")
        data_out = json.dumps(message)

        properties = pika.spec.BasicProperties(
            content_type="application/json", delivery_mode=2,message_id=messageId)
        channel.basic_publish(
            exchange=exchangeName, routing_key=routingKey, body=data_out, properties=properties)

        print("Disconnect...")
        # disconnect
        connect.close()
        return "Publish Success ! "+str(messageId)

    except Exception as e:
        if str(e) == "[Errno -2] Name or service not known":
            logger.error(
                "Can't connect with AMQP Broker , please check endpoint name in config file.")
        else:
            logger.error(str(e))
