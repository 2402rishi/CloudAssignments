from django.shortcuts import render,HttpResponse
from elasticsearch.client import IndicesClient
from elasticsearch import Elasticsearch,RequestsHttpConnection
from django.views.decorators.csrf import csrf_protect
import json
host = 'search-cloud9-e2ibuljqcyjtncoeb4jgj5obh4.us-east-1.es.amazonaws.com'
port = 443
@csrf_protect
def get_select_value(request):
	if "key_word" in request.POST : 
		selected_value = request.POST["key_word"]
		if selected_value :
			es = Elasticsearch(hosts=[{'host': host,'port':port}],use_ssl=True,verify_certs=True,connection_class=RequestsHttpConnection)
			res = es.search(size=5000,index="twitter", body={"query": {"match":{"text":selected_value}}})
			listOfDicts = [dict() for num in range (len(res['hits']['hits']))]
			for idx,elements in enumerate(listOfDicts) :
				sourceValue = res['hits']['hits'][idx]['_source']
				tempCoordinates=sourceValue['co-ordinates']
				tweetinfo=sourceValue['text']
				listOfDicts[idx]=dict(lng=tempCoordinates[0],lat=tempCoordinates[1])
			return render (request,"maps.html",{"lats" :listOfDicts})
	else:
		selected_value = None
	return render(request,"get_select_value.html", {'selected_value' : selected_value})

def maps(request) :
	return render(request, "maps.html")
