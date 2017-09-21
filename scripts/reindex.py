#!/usr/bin/python

import sys, os, json, urllib, urllib2, requests, codecs
from elasticsearch import Elasticsearch,helpers


es = Elasticsearch()
es.indices.create(index='labelindex1', ignore=400)
q={"query" : {"match_all" : {}}, "size" : 15000000}
#response= es.search(index="dbppredicateindex1", doc_type="text", body=query)
records = helpers.scan(es, query=q , index="dbmergedindex6", doc_type="text")

actions = []
count = 0
for hit in records:
    count += 1
    print count
    if len(hit['_source']['labels']['dbpediaLabel:en']) > 0:
        for word in hit['_source']['labels']['dbpediaLabel:en']:
            body={'urianalyzed':hit['_source']['uri'], 'dbpediaLabel':word}
            labelfound = True
            actions.append({"_op_type": "index", "_index": "dbpredicateindex5", "_type": "records", "_source": body})
    if len(hit['_source']['labels']['wikidataLabel:en']) > 0:
        for word in hit['_source']['labels']['wikidataLabel:en']:
            print word
            body={'urianalyzed':hit['_source']['uri'], 'wikidataLabel':word}
            labelfound = True
            actions.append({"_op_type": "index", "_index": "dbpredicateindex5", "_type": "records", "_source": body})
    if not labelfound:
        print hit['_source']['uri']
        q = """SELECT ?label WHERE 
           { <%s> rdfs:label ?label.}"""%hit['_source']['uri']

        #dbpedia
        url = "http://dbpedia.org/sparql"
        p = {'query': q}
        h = {'Accept': 'application/json'}
        r = requests.get(url, params=p, headers=h)
        d =json.loads(r.text)
        dbpedialabels = []
        oxfordlabels = []
        for row in d['results']['bindings']:
            if 'xml:lang' in row['label']:
                if row['label']['xml:lang'] == 'en':
                    if row['label']['value'] in dbpedialabels:
                        continue
                    body={'urianalyzed':hit['_source']['uri'], 'dbpediaLabel':row['label']['value']}
                    actions.append({"_op_type": "index", "_index": "dbpredicateindex5", "_type": "records", "_source": body}) 
                    labelfound = True
                    url = 'https://od-api.oxforddictionaries.com:443/api/v1/entries/en/' + row['label']['value'].lower() + '/synonyms'
                    r = requests.get(url, headers = {'app_id': '02757a3f', 'app_key': '9b620866d1baa9d80276ff790bcb2adb'})
                    #time.sleep(0.2)
                    try:
                        d = json.loads(r.text)
                        for result in d['results']:
                           for lexicalentry in result['lexicalEntries']:
                               for entry in lexicalentry['entries']:
                                   for sense in entry['senses']:
                                       if 'synonyms' in sense:
                                           for syn in sense['synonyms']:
                                               if syn['text'] in oxfordlabels:
                                                   continue
                                               oxfordlabels.append(syn['text'])
                                               body={'urianalyzed':hit['_source']['uri'], 'oxfordLabel':syn['text']}
                                               actions.append({"_op_type": "index", "_index": "dbpredicateindex5", "_type": "records", "_source": body})
                                               labelfound = True
                                       if 'subsenses' in sense:
                                           for subsense in sense['subsenses']:
                                               for synonym in subsense['synonyms']:
                                                   if synonym['text'] in oxfordlabels:
                                                       continue
                                                   oxfordlabels.append(synonym['text'])
                                                   body={'urianalyzed':hit['_source']['uri'], 'oxfordLabel':synonym['text']}
                                                   actions.append({"_op_type": "index", "_index": "dbpredicateindex5", "_type": "records", "_source": body})
                                                   labelfound = True
        
                    except Exception,e:
                        print e
                    
    if not labelfound:   
        body={'urianalyzed':hit['_source']['uri']}
        actions.append({"_op_type": "index", "_index": "dbpredicateindex5", "_type": "records", "_source": body})
    if len(actions) > 100:
        helpers.bulk(es, actions, index="dbpredicateindex5", doc_type="records")
        actions = []
   
helpers.bulk(es, actions, index="dbpredicateindex5", doc_type="records") 
