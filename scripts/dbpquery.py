#!/usr/bin/python

import sys, os, json, urllib, urllib2, requests
from elasticsearch import Elasticsearch

def query(q,epr,f='application/json'):
    try:
        params = {'query': q}
        params = urllib.urlencode(params)
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        request = urllib2.Request(epr+'?'+params)
        request.add_header('Accept', f)
        request.get_method = lambda: 'GET'
        url = opener.open(request)
        return url.read()
    except Exception, e:
        traceback.print_exc(file=sys.stdout)
        raise e 


f = open(sys.argv[1]) #json file
j = f.read()
d = json.loads(j)

x = {}

for q in d:
    for ent in q['entity mapping']:
        x[ent['uri']] =  ent['label']
    for pred in q['predicate mapping']:
        x[pred['uri']] = pred['label']

es = Elasticsearch()
es.indices.create(index='labelindex', ignore=400)

for k,v in x.iteritems():
    q = """SELECT ?label WHERE 
           { <%s> rdfs:label ?label.}"""%k

    url = "http://dbpedia.org/sparql"                                  
    p = {'query': q}             
    h = {'Accept': 'application/json'}                                 
    r = requests.get(url, params=p, headers=h)                         
    d =json.loads(r.text)
    for row in d['results']['bindings']:
        if row['label']['xml:lang'] == 'en':
            print k,' ',row['label']['value']
            es.index(index="labelindex", doc_type="text", body={"uri": k, "labels": {"dbpediaLabel:en":row['label']['value']}})
