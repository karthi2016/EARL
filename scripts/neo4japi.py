#!/usr/bin/python

from flask import request
from flask import Flask
import json,sys
import itertools
from neo4jrestclient.client import GraphDatabase,Path
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
from operator import itemgetter

db = GraphDatabase("http://localhost:7474", username="neo4j", password=sys.argv[1])


app = Flask(__name__)


def checkPath((path, sequence)):
    queryString = ''
    nodeEdgeSequence = [] # neo4j match query must be of the form node-edge-node
    count = 0
    for uri in path:
        if '/property/' in uri or '/ontology/' in uri:
            if count == 0:
                queryString += '()-[:R {uri: \"%s\"}]-'%uri
                nodeEdgeSequence.append('E')
            if count > 0 and nodeEdgeSequence[-1] == 'E':
                queryString += '()-[:R {uri: \"%s\"}]-'%uri
                nodeEdgeSequence.append('E')
            if count > 0 and nodeEdgeSequence[-1] == 'N':
                queryString += '[:R {uri: \"%s\"}]-'%uri
                nodeEdgeSequence.append('E')
        if '/resource/' in uri:
            if count == 0:
                queryString += '(:Node {uri: \"%s\"})-'%uri
                nodeEdgeSequence.append('N')
            if count > 0 and nodeEdgeSequence[-1] == 'N':
                queryString += '[:R]-(:Node {uri: \"%s\"})-'%uri
                nodeEdgeSequence.append('N')
            if count > 0 and nodeEdgeSequence[-1] == 'E':
                queryString += '(:Node {uri: \"%s\"})-[:R]-'%uri
                nodeEdgeSequence.append('E')

        count += 1
    if queryString[-2:] == ']-':
        queryString += '()'
    if queryString[-2:] == ')-':
        queryString = queryString[:-1]
    queryString = 'match p = '+queryString+'  return length(p)'
    results = []
    try:
        results = db.query(queryString, returns=(str))
        if len(results) > 0:
            minlength = 999
            for r in results:
                print int(r[0])
                if int(r[0]) < minlength:
                    minlength = int(r[0])
            print minlength
            return (minlength, sequence, path, queryString)

    except Exception,e:
        print e
    return (0, sequence, path, queryString)

def noEntityInPath(cartesianProductItem):
    for item in cartesianProductItem:
        if '/resource/' in item:
            return False
    return True


@app.route('/findPaths', methods=['POST'])
def findPaths():
    pool = ThreadPool(20)
    d = request.get_json(silent=True)
    lists = []
    for k,v in d.iteritems():
        lists.append(d[k])
    sequence = range(len(lists))
    sequences = itertools.permutations(sequence)
    pathsToBeChecked = []
    for sequence in sequences:
        print "For permuted sequence ",sequence
        permutedlists = []
        for index in sequence:
            permutedlists.append(lists[index])
        for cartesianProductItem in itertools.product(*permutedlists):
            if noEntityInPath(cartesianProductItem):
                continue
            pathsToBeChecked.append((cartesianProductItem,sequence))
    results = pool.map(checkPath, pathsToBeChecked)
    pool.close()
    pool.join()
    response = []
    for record in results:
        item = {}
        if record[0] > 0:
            item['neo4jQuery'] = record[3]
            item['pathLength'] = record[0]
            item['permuteSequence'] = list(record[1])
            item['urisMatched'] = list(record[2])
            response.append(item)
    newresponse = sorted(response, key=itemgetter('pathLength'))
    return json.dumps(newresponse)


if __name__ == '__main__':
    app.run(debug=True)
