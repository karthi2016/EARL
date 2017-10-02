#!/usr/bin/python

from flask import request
from flask import Flask
import json,sys
import itertools
from neo4jrestclient.client import GraphDatabase,Path
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

pool = ThreadPool(20)

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
    queryString = 'match p = '+queryString+' return count(p)'
    results = []
    try:
        results = db.query(queryString, returns=(str))
        if len(results) > 0:
            if int(results[0][0]) > 0:
                print "Query String ",queryString
                print results[0][0]
                return (queryString, int(results[0][0]), sequence, path)

    except Exception,e:
        print e
    return (queryString, 0, sequence, path)

def noEntityInPath(cartesianProductItem):
    for item in cartesianProductItem:
        if '/resource/' in item:
            return False
    return True


@app.route('/findPaths', methods=['POST'])
def findPaths():
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
        if record[1] > 0:
            item['neo4jQuery'] = record[0]
            item['pathsCount'] = record[1]
            item['permuteSequence'] = list(record[2])
            item['urisMatched'] = list(record[3])
            response.append(item)
    return json.dumps(response)




if __name__ == '__main__':
    app.run(debug=True)
