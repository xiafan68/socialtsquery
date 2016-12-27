# encoding:utf8
import sys
import os
from os.path import dirname
from os.path import join
import json
from optparse import OptionParser
from operator import itemgetter
from itertools import izip, count, izip_longest

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
    
    sys.argv = ["-i", "/Users/kc/快盘/dataset/time_series/nqueryseed.txt", "-o", "/Users/kc/快盘/dataset/twitter_expr/keywordsbycount"]
    sys.argv = ["-i", "/Users/kc/快盘/dataset/twitter_expr/queryseed.txt", "-o", "/Users/kc/快盘/dataset/twitter_expr"]
    
    (options, args) = parser.parse_args(sys.argv)

    print args
    if len(args) == 1:
        parser.print_help()
        exit
    
    t = Transformer(options.inputs, options.odir)
    t.loadRecs()

# python etoffset.py -p hpl_ns hpl_s ipl_s ipl_ns -i $idir  -f total_time -x width -y k -i /Users/xiafan/快盘/dataset/exprresult/merge/raw -o /Users/xiafan/快盘/dataset/exprresult/merge/transform/ -s 0 2 12 24

# python etoffset.py -i /Users/xiafan/快盘/dataset/exprresult/merge/raw -o /Users/xiafan/快盘/dataset/exprresult/merge/transform/ -k -t io -f basetree atomic segstore LIST_IO -s 0 2 12 24  -p hpl_ns hpl_s ipl_s ipl_ns -x width -y k

# python etoffset.py -i /Users/xiafan/快盘/dataset/exprresult/merge/raw -o /Users/xiafan/快盘/dataset/exprresult/merge/transform/ -k -t time -f basetree_time atomic_time segstore_time compute_score LIST_IO_time de_sketch other_cost -s 0 2 12 24  -p hpl_ns hpl_s ipl_s ipl_ns -x width -y k
