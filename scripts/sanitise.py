#!/usr/bin/python

import sys, os, json, urllib, urllib2, requests, codecs
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


f = codecs.open(sys.argv[1], "r", "utf-8") #json file
j = f.read()
d = json.loads(j)

x = {}

for q in d:
    for ent in q['entity mapping']:
        x[ent['uri']] =  ent['label']
    for pred in q['predicate mapping']:
        x[pred['uri']] = pred['label']

f.close()

es = Elasticsearch()

f = codecs.open(sys.argv[2], "r", "utf-8")
s = f.read()
j = json.loads(s)

for d in j['EntityLabels']:
    #print d['DbpediaUri']
    res = es.search(index="labelindex1", doc_type="text", body={"query": {"term": {"uri": d['DbpediaUri']}}})
    hid = None
    if res['hits']['total'] > 0:
        hid = res['hits']['hits'][0]['_id']
    if hid:
        print d['Wikidatalabel']
        es.update(index="labelindex1", doc_type="text", id=hid,  body={"doc":{"labels": {"wikidata:en":d['Wikidatalabel']}}})

#for line in f.readlines():
#    k = line.split('::')[0].strip()
#    vs = [X.strip() for X in line.split('::')[1][1:-2].split(',')]
#    hid = None
#    res = es.search(index="labelindex1", doc_type="text", body={"query": {"term": {"uri": k}}})
#    if res['hits']['total'] > 0:
#        hid = res['hits']['hits'][0]['_id']
#        dbplabel = res['hits']['hits'][0]['_source']['labels']['dbpediaLabel:en']
#    wikidatalabels = []
#    if k in x:
#        for v in vs:
#            if v == x[k]:
#                continue 
#            wikidatalabels.append(v)
#    if hid:
#        print hid
#        #es.update(index="labelindex1", doc_type="text", id=hid,  body={"doc":{"labels": {"wikidata:en":wikidatalabels}}})
#            #print x[k],' ',v 
#

#es.indices.create(index='labelindex', ignore=400)
#es.index(index="labelindex", doc_type="text", body={"uri": k, "labels": {"dbpediaLabel:en":row['label']['value']}})
