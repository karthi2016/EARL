#!/usr/bin/python

import sys, os, time
import itertools
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", sys.argv[1]))

d1 = { "ex1": ["http://dbpedia.org/resource/Barack_Obama","http://dbpedia.org/resource/Chess", "http://dbpedia.org/property/skills"] , "ex2": ["http://dbpedia.org/resource/Philadelphia_Spinners", "http://dbpedia.org/property/president", "http://dbpedia.org/property/publisher"], "ex3": ["http://dbpedia.org/resource/Arnold_Ziffel", "http://dbpedia.org/ontology/academicAdvisor", "http://dbpedia.org/ontology/almaMater"]}


lists = []

for k,v in d1.iteritems():
    lists.append(d1[k])

sequence = range(len(lists))

sequences = itertools.permutations(sequence)


def queryBuilder(path):
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
    queryString = 'match p = '+queryString+' return p'
    #print "Query String ",queryString
    results = []
    try:
        global driver
        with driver.session() as session:
            with session.begin_transaction() as tx:
                results = session.run(queryString)
    except Exception,e:
        print e
    try:
        pathArr = [] 
        for record in results:
            relationships = record["p"].relationships
            nodes = record["p"].nodes
            path = ""
            for i in (range(len(relationships))):
                path += "{0}-[{1}]->".format(nodes[i]["name"], relationships[i].type)
            path += nodes[-1]["name"]
            pathArr.append(path)
    except Exception,e:
        print e


def checkPath(path):
    queryString = queryBuilder(path)

def noEntityInPath(cartesianProductItem):
    for item in cartesianProductItem:
        if '/resource/' in item:
            return False
    return True


for sequence in sequences:
    print "For permuted sequence ",sequence
    permutedlists = []
    for index in sequence:
        permutedlists.append(lists[index])
    for cartesianProductItem in itertools.product(*permutedlists):
        if noEntityInPath(cartesianProductItem):
            continue
        checkPath(cartesianProductItem)
        

#results = session.run("match(n) return count(n)")

#for record in results:
#    print record

session.close()
