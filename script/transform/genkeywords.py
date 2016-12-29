# encoding:utf8
import sys
import os
from os.path import dirname
from os.path import join
import json
from optparse import OptionParser
from operator import itemgetter
from itertools import izip, count, izip_longest
from timeit import itertools


class TripleKeywordGen(object):
    def __init__(self, iDir):
        self.iDir = iDir.decode("utf8")

    def loadRecs(self):
        input = open(self.iDir, "r")
        edges = []
        vertexToEdge = {}
        for index, line in izip(count(), input.readlines()):
            fields = line.rstrip().split("\t")
            edge = (index, fields[-2], fields[-1], set(fields[0:-3]))
            edges.append(edge)
            if not(edge[1] in vertexToEdge):
                vertexToEdge[edge[1]] = []
            vertexToEdge[edge[1]].append(edge)
            if not(edge[2] in vertexToEdge):
                vertexToEdge[edge[2]] = []
            vertexToEdge[edge[2]].append(edge)   
        
        output = open(join(dirname(self.iDir), "triplewords.txt"), 'w')
        timePointMapping={}
        tripleEdgeList=[]
        for i in range(len(edges)):
            edge = edges[i]
            vertices = [edge[1], edge[2]]
            for vertex in vertices:
                for adjEdge in vertexToEdge.get(vertex, []):
                    if adjEdge.index > edge.index:
                        timePoints = edge[3].intersection(adjEdge[3])
                        if len(timePoints) > 0:
                            oVertex = adjEdge[1]
                            if oVertex == vertex:
                                oVertex = adjEdge[2]
                            tripleEdge = (edge[1], edge[2], oVertex)
                            if not(tripleEdge in timePointMapping):
                                timePointMapping[tripleEdge]=timePoints
                                tripleEdgeList.append(tripleEdge)
        for edge in tripleEdgeList:
            output.write("\t".join([str(timePoint) for timePoint in timePointMapping[edge]]))
            output.write("\t")
            output.write("\t".join(edge))
            output.write("\n")
        output.close()
                    
            
            
def put(dict, value):
    rDict[dim]
    curDict[dims[-1]] = value
    
def cmpByRankList(a, b):
    if len(a) == 0:
        return 1
    elif len(b) == 0:
        return -1
    else:
        for i in range(len(a)):
            if i >= len(b):
                return -1
            elif a[i][0] > b[i][0]:
                return 1
            elif a[i][0] < b[i][0]:
                return -1
            elif a[i][1] > b[i][1]:
                return -1
            elif a[i][1] < b[i][1]:
                return 1
    return 0           

def cmpWordRank(x, y):
    x = x[1]
    y = y[1]
    for a, b in izip_longest(x, y):
        if a is None:
            return 1
        elif b is None:
            return -1
        elif a[0] == b[0]:
            if a[1] != b[1]:
                return b[1] - a[1]
        else:
            return a[0] - b[0]
    return 0
                        
def groupBy(pairs):
    from collections import defaultdict
    counter = defaultdict(lambda:0)
    for pair in pairs:
        counter[pair[0]] += pair[1]
    return counter.items()

class Transformer(object):
    def __init__(self, iDir, oDir):
        self.iDir = iDir.decode("utf8")
        self.oDir = oDir
        self.edge = []
        self.startAdj = {}
        self.endAdj = {}
    def loadRecs(self):
        input = open(self.iDir, "r")
        wordTimeList = {}
        wordRankList = {}
        for index, line in izip(count(), input.readlines()):
            rec = line.rstrip().split("\t")
            if not(rec[0] in wordTimeList):
                wordTimeList[rec[0]] = []
            wordTimeList.get(rec[0], []).extend(rec[2: ])
            if not(rec[1] in wordTimeList):
                wordTimeList[rec[1]] = []
            wordTimeList.get(rec[1], []).extend(rec[2: ])
            if not(rec[0] in wordRankList):
                wordRankList[rec[0]] = []
            wordRankList.get(rec[0]).append(index)
            if not(rec[1] in wordRankList):
                wordRankList[rec[1]] = []
            wordRankList.get(rec[1], []).append(index)
        input.close()
        
        for keyword in wordTimeList.keys():
            wordTimeList[keyword] = sorted(groupBy(map(lambda x:(x, 1), wordTimeList[keyword])), key=itemgetter(1))
        for keyword in wordRankList.keys():
            wordRankList[keyword] = sorted(groupBy(map(lambda x:(x, 1), wordRankList[keyword])), key=itemgetter(0))
        
        output = open(join(dirname(self.iDir), "singleword.txt"), 'w')
        for item in  sorted(wordRankList.items(), cmpWordRank):
            output.write(item[0] + "\t")
            output.write("\t".join([str(timepoint[0]) for timepoint in wordTimeList[item[0]]]))
            output.write("\n")
        output.close()
        
     
if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-i", "--idir", dest="inputs", help="input directory")
    parser.add_option("-o", "--odir", dest="odir", help="output directory")
    parser.add_option("-t", "--triple", action="store_true", dest="isTriple", help="generate triple keywords")
    
    sys.argv = ["-i", "/Users/kc/快盘/dataset/time_series/nqueryseed.txt", "-o", "/Users/kc/快盘/dataset/twitter_expr/keywordsbycount"]
    sys.argv = ["-i", "/Users/kc/快盘/dataset/twitter_expr/queryseed.txt", "-o", "/Users/kc/快盘/dataset/twitter_expr"]
    sys.argv = ["-i", "/Users/kc/快盘/dataset/twitter_expr/queryseed_swap.txt", "-o", "/Users/kc/快盘/dataset/twitter_expr", "-t"]
    #sys.argv = ["-i", "/Users/kc/快盘/dataset/time_series/nqueryseed_swap.txt", "-o", "/Users/kc/快盘/dataset/timeseries", "-t"]
    
    (options, args) = parser.parse_args(sys.argv)

    print args
    if len(args) == 1:
        parser.print_help()
        exit
    
    if options.isTriple:
        gen = TripleKeywordGen(options.inputs)
        gen.loadRecs()
    else:
        t = Transformer(options.inputs, options.odir)
        t.loadRecs()
# python etoffset.py -p hpl_ns hpl_s ipl_s ipl_ns -i $idir  -f total_time -x width -y k -i /Users/xiafan/快盘/dataset/exprresult/merge/raw -o /Users/xiafan/快盘/dataset/exprresult/merge/transform/ -s 0 2 12 24

# python etoffset.py -i /Users/xiafan/快盘/dataset/exprresult/merge/raw -o /Users/xiafan/快盘/dataset/exprresult/merge/transform/ -k -t io -f basetree atomic segstore LIST_IO -s 0 2 12 24  -p hpl_ns hpl_s ipl_s ipl_ns -x width -y k

# python etoffset.py -i /Users/xiafan/快盘/dataset/exprresult/merge/raw -o /Users/xiafan/快盘/dataset/exprresult/merge/transform/ -k -t time -f basetree_time atomic_time segstore_time compute_score LIST_IO_time de_sketch other_cost -s 0 2 12 24  -p hpl_ns hpl_s ipl_s ipl_ns -x width -y k
