import boto3
from multiprocessing.dummy import Pool as ThreadPool
from textblob import TextBlob
import urllib2
import time
import sys
import json
import re
import boto3
import threading
from elasticsearch.client import IndicesClient
from elasticsearch import Elasticsearch,RequestsHttpConnection
host = 'search-cloudsentiment-ogi2jvxomkfs7cai4hqg2tuhtq.us-west-2.es.amazonaws.com'
port = 443
#Instantiating Elasticsearch
ES_CLIENT = Elasticsearch(
        hosts=[{'host': host,'port':port}],
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
        )
indices_client = IndicesClient(client=ES_CLIENT)
i=1
client = boto3.client('sqs')
response = client.get_queue_url(
    QueueName='RV',
    QueueOwnerAWSAccountId=''
)
def starter(numbers):
	while (True):
		try:
			message=client.receive_message(QueueUrl=response['QueueUrl'],MessageAttributeNames=['tweets','location'],MaxNumberOfMessages=10)
			handle=message['Messages'][0]['ReceiptHandle']
			print message['Messages'][0]
			text=message['Messages'][0]['MessageAttributes']['tweets']['StringValue']
			text=_removeNonAscii(text)
			testimonial= TextBlob(text)
			polarity=testimonial.sentiment.polarity
			coordinates= message['Messages'][0]['MessageAttributes']['location']['StringValue']
			print coordinates
			if polarity == 0:
				polarity_expression='neutral'
			elif polarity >0:
				polarity_expression='positive'
			else :
				polarity_expression='negative'
			jsonObject = {}
			jsonObject['co-ordinates'] = coordinates
			jsonObject['text']=text
			jsonObject['polarity']= polarity_expression
			json_data = json.dumps(jsonObject)
			global i
			ES_CLIENT.index(index="twitter", doc_type="tweets",id=i,body=json_data)
			i=i+1
			abc = client.delete_message(QueueUrl=response['QueueUrl'],ReceiptHandle=str(handle))
			print abc
		except :
			continue
		

			
			
def threadPool(numbers,threads):
	pool=ThreadPool ()
	result=pool.map(starter,numbers)
	pool.close()
	pool.join()
	return result
def _removeNonAscii(s): return "".join(i for i in s if ord(i)<128)
if __name__== '__main__':
	numbers=[1,2,3,4,5,6]
	for n in range(50):
		tweet_text=threadPool(numbers,10)
