#!/usr/bin/env python
# coding: utf-8

# # This is a simple example of Consumer in Kafka by Xenias Dimitrios

# ### Before running the cells, you have to start the zookeeper server,the brokers of Kafka cluster and the mongodb service

# In[ ]:


get_ipython().system('pip3 install kafka-python')
get_ipython().system('pip3 install pymongo')


# ===========================
# #### Cosnumer Snippet Code
# ===========================

# In[ ]:


from kafka import KafkaConsumer
from pymongo import MongoClient


# In[ ]:


from json import loads

TOPIC="Covid-19"

def connect_kafka_consumer():
    consumer = None
    try:
        consumer = KafkaConsumer(TOPIC, bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 group_id='my-group',
                                 value_deserializer= lambda x: loads(x.decode('utf-8')))
    except Exception as ex:
        print('Exception while connecting KafkaProducer')
        print(str(ex))
    finally:
        return consumer
                                 
def connect_to_mongo():
    client = None
    try:
        client = MongoClient('localhost:27017')
        collection = client.Covid.Covid
    except Exception as ex:
        print('Excpetion while connecting to MongoDB')
        print(str(ex))
    finally:
        return collection


# In[ ]:


consumer_instance = connect_kafka_consumer()
collection = connect_to_mongo()
for message in consumer_instance:
    print('Message Consumed Successfully')
    print(message.value)
    collection.insert_one(message.value[0])


# In[ ]:




