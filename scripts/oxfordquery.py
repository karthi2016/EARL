#!/usr/bin/python

import sys, os, json, urllib, urllib2, requests, time
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
    #for ent in q['entity mapping']:
    #    x[ent['uri']] =  ent['label']
    for pred in q['predicate mapping']:
        x[pred['uri']] = ''

es = Elasticsearch()


app_id = ''
app_key = ''

for k,v in x.iteritems():
    q = """SELECT ?label WHERE 
           { <%s> rdfs:label ?label.}"""%k
    url = "http://131.220.153.66:7890/sparql"
    p = {'query': q}             
    h = {'Accept': 'application/json'}                                 
    r = requests.get(url, params=p, headers=h)
    print("code {}\n".format(r.status_code))
    d =json.loads(r.text)
    for row in d['results']['bindings']:
        if row['label']['xml:lang'] == 'en':
            label = row['label']['value']
            if ' ' not in label:
                oxfordurl = url = 'https://od-api.oxforddictionaries.com:443/api/v1/entries/en/' + label.lower() + '/synonyms'
                r = requests.get(oxfordurl, headers = {'app_id': app_id, 'app_key': app_key})
                time.sleep(1)
                try:
                    d = json.loads(r.text)
                except Exception,e:
                    print e
                    continue
                synlist = []
                for result in d['results']:
                    for lexicalentry in result['lexicalEntries']:
                        for entry in lexicalentry['entries']:
                            for sense in entry['senses']:
                                if 'synonyms' in sense:
                                    for syn in sense['synonyms']:
                                        synlist.append(syn['text'])
                                if 'subsenses' in sense:
                                    for subsense in sense['subsenses']:
                                        for synonym in subsense['synonyms']:
                                            synlist.append(synonym['text'])

                print k,' ',synlist
                
            #es.index(index="labelindex", doc_type="text", body={"uri": k, "labels": {"dbpediaLabel:en":row['label']['value']}})
