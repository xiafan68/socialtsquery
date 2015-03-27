#encoding:utf8
import sys
import os
import json

class Transformer(object):
    def __init__(self, iFile):
        self.iFile = iFile

    def loadRecs(self):
        input = open(self.iFile, "r")
        self.recs=[]
        for line in input.readlines():
            self.recs.append(json.loads(line))
        input.close()
    
    def transform(self, oFile, xaxis, yaxis, fields):
        matrix={}
        output = open(oFile, "w")
        for rec in self.recs:
            rec = rec.copy()
            row = matrix.get(rec[xaxis],{})
            row[rec[yaxis]]= rec
            if rec[xaxis] not in matrix:
                matrix[rec[xaxis]]=row
            del rec[xaxis]
            del rec[yaxis]
        
        for x in sorted(matrix.keys()):
            row = matrix[x]
            outLine = "%s\t%s\n"%(str(x),"\t".join(["%.3f"%row[col].get(field, 0.0) for col in sorted(row.keys()) for field in fields ]))
            output.write(outLine)
        output.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "input file, output dir"
        exit
    
    #iFile="/Users/xiafan/快盘/workspace/intervaltopk/exprresult/hpl_s_pop_o1w.txt"
    iFile = sys.argv[1].decode('utf-8')
    baseName = os.path.basename(os.path.splitext(iFile)[0])
    print "basename",baseName
    t = Transformer(iFile)
    t.loadRecs()
    #oFile="/Users/xiafan/快盘/workspace/intervaltopk/exprresult/hpl_s_pop_k_w_o1w.txt"

    baseDir = os.path.join(sys.argv[2].decode('utf8'), "k_w","time")
    print baseDir
    if not os.path.exists(baseDir):
       os.makedirs(baseDir)
        
    oFile = os.path.join(baseDir,baseName+ ".txt")
    t.transform(oFile,"k","width",["total", "update", "atomic_time"])
    #I/O
    baseDir = os.path.join( sys.argv[2].decode('utf8'), "k_w","IO")
    if not os.path.exists(baseDir):
        os.makedirs(baseDir) 
    oFile = os.path.join(baseDir,baseName+ "_IO.txt")
    t.transform(oFile,"k","width",["atomic"])
    
    baseDir = os.path.join( sys.argv[2].decode('utf8'), "w_k","time")
    if not os.path.exists(baseDir):
        os.makedirs(baseDir)
    oFile =os.path.join(baseDir,baseName+ ".txt")
    t.transform(oFile,"width","k",["total", "update", "atomic_time"])
    #I/O
    baseDir = os.path.join(sys.argv[2].decode('utf8'), "w_k","IO")
    if not os.path.exists(baseDir):
        os.makedirs(baseDir)
    oFile = os.path.join(baseDir,baseName+ ".txt")
    t.transform(oFile,"width","k",["atomic"])
