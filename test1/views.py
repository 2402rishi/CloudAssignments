from django.shortcuts import render,HttpResponse
from elasticsearch.client import IndicesClient
from elasticsearch import Elasticsearch,RequestsHttpConnection
from django.views.decorators.csrf import csrf_protect
import json
host = 'search-cloudsentiment-ogi2jvxomkfs7cai4hqg2tuhtq.us-west-2.es.amazonaws.com'
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
				tempCoordinates=sourceValue['co-ordinates'].strip("'").strip('[').strip(']').split(',')
				print sourceValue
				tweetinfo=sourceValue['text']
				polarity=sourceValue['polarity']
				if polarity == 'neutral':
					marker_color="info-i_maps.png"
				elif polarity == 'positive':
					marker_color='parking_lot_maps.png'
				else:
					marker_color="library_maps.png"
				listOfDicts[idx]=dict(lng=float(tempCoordinates[0]),lat=float(tempCoordinates[1]),color=marker_color)
				print listOfDicts
			return render (request,"maps.html",{"lats" :listOfDicts})
	else:
		selected_value = None
	return render(request,"get_select_value.html", {'selected_value' : selected_value})

def maps(request) :
	return render(request, "maps.html")
