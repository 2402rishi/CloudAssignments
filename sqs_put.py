import tweepy
import json
import boto3
from elasticsearch.client import IndicesClient
from elasticsearch import Elasticsearch,RequestsHttpConnection
from boto.sqs.message import Message
import boto.sqs
con_key ='BVSYc1ik4xcj0lWOHbrkW638l'
con_secret ='WbXAkyrqihU1rEsBsU0s2J8Ucx3quY4Qxfs1L55jDnaEgnrX13'
acess_token ='804568838-zKDbr4nf261maYNwCrMAQfb2m6aoXIaQ9MdCbcdo'
acess_secret = 'vj7vsczhR2hHiZq7f212CsNNLpa7AZwN0WWVZACGAR4MT'
conn = boto.sqs.connect_to_region(
"us-west-2",
aws_access_key_id='AKIAIUTFZRPGITIJCZEA',
aws_secret_access_key='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')
#q = conn.create_queue('RV')
#sqs = boto3.resource('sqs', region_name="us-west-2")
#q = conn.get_queue_by_name(QueueName='RV')

q = conn.get_queue('RV')
class MyStreamListener(tweepy.StreamListener):
    def on_data(self, raw_data):
        data = json.loads(raw_data)
        if data.get('place') and data.get('user').get('lang') == 'en' :
            saveData(data)

def saveData(data):
    coordinates = data.get('place').get('bounding_box').get('coordinates')[0][0]
    l=data.get('user').get('lang')
    text= data.get('text')
    print "HI"
    m=Message()
    m.set_body(text)
    m.message_attributes={
    'language': {
    'data_type': 'String',
    'string_value': l
    },
    'location': {
    'data_type': 'String',
    'string_value': coordinates
    },      
    'tweets':{

    'data_type':'String',
    'string_value': text
    }
    }

    q.write(m)
auth = tweepy.OAuthHandler('BVSYc1ik4xcj0lWOHbrkW638l', 'WbXAkyrqihU1rEsBsU0s2J8Ucx3quY4Qxfs1L55jDnaEgnrX13')
auth.set_access_token('804568838-zKDbr4nf261maYNwCrMAQfb2m6aoXIaQ9MdCbcdo', 'vj7vsczhR2hHiZq7f212CsNNLpa7AZwN0WWVZACGAR4MT')
api = tweepy.API(auth)
myStreamListener = MyStreamListener()

while True:
    try :
        myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
        (myStream.filter(track=["trump","modi","immigrants","holi","PokemonGo","USA","Election","IMB","Apple","Soccer"]))
    except :
        # Or however you want to exit this loop
        myStream.disconnect()
        


