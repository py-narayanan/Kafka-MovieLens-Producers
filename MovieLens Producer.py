#!/usr/bin/env python
# coding: utf-8

# In[23]:


from kafka import KafkaConsumer, KafkaProducer
import time
import pandas as pd
from json import dumps


# In[24]:


from kafka import KafkaConsumer, KafkaProducer
movie_csv = pd.read_csv("movie.csv", delimiter= "','", engine='python')
movie_json_convert = movie_csv.to_json("movies.json")


# In[25]:


KAFKA_TOPIC_NAME = "numtest"
KAFKA_BOOTSTRAP_SERVER_CONN = "localhost:9092"

kafka_producer_object = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER_CONN,
              value_serializer=lambda x: dumps(x).encode('utf-8'))


# In[27]:


movies_json = pd.read_json("movies.json")
movie_list= movies_json.to_dict(orient="records")


# In[29]:


for movie in movie_list:
    print("Message to be send : ", movie)
    kafka_producer_object.send(KAFKA_TOPIC_NAME,value=movie)
    time.sleep(3)


# In[ ]:




