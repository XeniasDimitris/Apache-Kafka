#!/usr/bin/env python
# coding: utf-8

# # This is a simple example of Producer in Kafka by Xenias Dimitrios

# ### Before running the cells, you have to start the zookeeper server and the brokers of Kafka cluster at your local machine

# In[ ]:


get_ipython().system('pip3 install kafka-python')
get_ipython().system('pip3 install requests ')


# In[ ]:


import requests
headers = {'x-rapidapi-host':'covid-19-data.p.rapidapi.com', 'x-rapidapi-key':'642fed79f1msh04682ecfc9ca8f6p161da1jsn3b123c3a533d'}


# In[ ]:


def fetch_data(date,country):
    params = {'date': date, 'name': country}
    res = requests.get('https://covid-19-data.p.rapidapi.com/report/country/name', params=params, headers=headers)
    return res.json()


# ===========================
# #### Producer Snippet Code
# ===========================

# In[ ]:


from kafka import KafkaProducer
import json


# In[ ]:




def connect_kafka_producer():
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer= lambda x: json.dumps(x).encode('utf-8'))
    except Exception as ex:
        print('Exception while connecting KafkaProducer')
        print(str(ex))
    finally:
        return producer
    
def publish_message(producer_instance,topic_name,value):
    try:
        producer_instance.send(topic_name, value=value)
        print('Message published successfully')
    except Exception as ex:
        print('Exception while publishing message')
        print(str(ex))
       


# In[ ]:


import datetime
def date_window(start = '2020-1-2', finish = str(datetime.date.today())):
    window = []
    step = datetime.timedelta(days=1)
    start = start.split('-')
    startdate = datetime.date(int(start[0]), int(start[1]), int(start[2]))
    finish = finish.split('-')
    finishdate = datetime.date(int(finish[0]), int(finish[1]), int(finish[2]))
    x = startdate
    while x!=finishdate:
        window.append(str(x))
        x += step
    window.append(str(finishdate))
    return window


# In[ ]:


from time import sleep
producer_instance = connect_kafka_producer()

WINDOW = date_window('2020-05-1','2020-05-1')
COUNTRY = 'italy'
TOPIC = 'Covid-19'

for date in WINDOW:
    data = fetch_data(date,COUNTRY)
    print(data)
    publish_message(producer_instance,TOPIC,data)
    sleep(1)

