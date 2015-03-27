# encoding:utf8
import sys
import os
import json
from optparse import OptionParser

def put(dict, dims, value):
    curDict=dict
    for dim in dims[0:-1]:
        if not (dim in curDict):
            preDict = curDict
            curDict = {}
            preDict[dim]=curDict
        else:
            curDict=curDict[dim]
    curDict[dims[-1]]=value
class Transformer(object):
    def __init__(self, iDir, prefixs, starts):
        self.iDir = iDir.decode("utf8")
        self.prefixs = prefixs
        self.starts = starts

                
    def loadRecs(self, xName, yName, isStack):
        self.xName = xName
        self.yName = yName
        self.recs = {}
        for start in self.starts:
            for prefix in self.prefixs:
                iFile = os.path.join(self.iDir, "%s_o%sw.txt" % (prefix, start))
                #print "iFile ", iFile
                input = open(iFile, "r")
                for line in input.readlines():
                    rec = json.loads(line)
                    if isStack:
                        put(self.recs, (rec[xName], start, prefix, rec[yName]), rec)
                    else:
                        put(self.recs, (rec[xName], rec[yName], start, prefix), rec)
                input.close()
                        
    def transform(self, oDir, fields):
        oDir = oDir.decode("utf8")
        oDir = os.path.join(oDir, "offset")
        print "output ", oDir
        if not os.path.exists(oDir):
            print "makedir ", oDir
            os.makedirs(oDir)
        for (dim1, value1) in self.recs.items():
            for (dim2, value2) in value1.items():
                oFile = os.path.join(oDir, "voffset_%s_%s_%s_%s.txt" % (self.xName, str(dim1), self.yName, str(dim2)))
                output = open(oFile, "w")
                for (offset, recs) in sorted(value2.items(), lambda x,y:cmp(int(x[0]),int(y[0]))):
                    line=[str(offset)]
                    for app in self.prefixs:
                        rec=recs[app]
                        line.append("\t".join([str(rec[field]) for field in fields]))
                    line.append("\n")
                    output.write("\t".join(line))
                output.close()
  
    def writeHeader(self, output):
        output.write("header\n")
        output.write("header\n")

    def transformForStack(self, oDir, fields, tag):
        oDir = oDir.decode("utf8")
        oDir = os.path.join(oDir, "offset_"+tag)
        print "output ", oDir
        if not os.path.exists(oDir):
            print "makedir ", oDir
            os.makedirs(oDir)
        for (key, value) in sorted(self.recs.items(), lambda x, y :cmp(int(x[0]), int(y[0]))):
            #print key
            oFile = os.path.join(oDir,"offset_%s_%s_%s_%s.txt" % (self.yName, self.xName, str(key),tag))
            output = open(oFile, "w")
            self.writeHeader(output)
            for (offset, typesRecs) in sorted(value.items(), lambda x, y :cmp(int(x[0]), int(y[0]))):
                #print offset,typesRecs
                for app in self.prefixs:
                    line=[str(offset),str(app.replace("_","\\\\_"))]
                    rec=typesRecs[app]
                    for (key, value) in rec.items():
                        #line.append(str(key))
                        line+=[str(value[field]) for field in fields]
                    line.append("\n")
                    output.write("\t".join(line))
            output.close()
                                 
def vararg_callback(option, opt_str, value, parser):
     assert value is None
     value = []

     for arg in parser.rargs:
         # stop on --foo like options
         if arg[:2] == "--" and len(arg) > 2:
             break
         # stop on -a, but not on -3 or -3.0
         if arg[:1] == "-" and len(arg) > 1:
             break
         value.append(arg)

     del parser.rargs[:len(value)]
     setattr(parser.values, option.dest, value)
     
if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-i", "--idir", dest="inputs", help="input directory")
    parser.add_option("-p", "--prefix", action="callback", callback=vararg_callback, dest="prefix", help="prefix of input files")
    parser.add_option("-s", "--start", dest="start", action="callback", callback=vararg_callback, help="offset from the start time of event")
    parser.add_option("-o", "--odir", dest="odir", help="output directory")
    parser.add_option("-x", "--xaxis", dest="xaxis", help="x axis")
    parser.add_option("-y", "--yaxis", dest="yaxis", help="y axis")
    parser.add_option("-f", "--fields", dest="fields", action="callback", callback=vararg_callback, help="fields")
    parser.add_option("-t", "--tag", dest="tag", help="tag that will appended to the output file name")
    parser.add_option("-k", "--stack", dest="stack", action="store_true", default=False, help="if stacked")
    
    
    (options, args) = parser.parse_args(sys.argv)
    print args
    if len(args) == 1:
        parser.print_help()
        exit
    
    t = Transformer(options.inputs, options.prefix, options.start)
    t.loadRecs(options.xaxis, options.yaxis,options.stack)
    if options.stack:
        t.transformForStack(options.odir, options.fields, options.tag)
    else:
        t.transform(options.odir, options.fields)

#python etoffset.py -p hpl_ns hpl_s ipl_s ipl_ns -i $idir  -f total_time -x width -y k -i /Users/xiafan/快盘/dataset/exprresult/merge/raw -o /Users/xiafan/快盘/dataset/exprresult/merge/transform/ -s 0 2 12 24

#python etoffset.py -i /Users/xiafan/快盘/dataset/exprresult/merge/raw -o /Users/xiafan/快盘/dataset/exprresult/merge/transform/ -k -t io -f basetree atomic segstore LIST_IO -s 0 2 12 24  -p hpl_ns hpl_s ipl_s ipl_ns -x width -y k

#python etoffset.py -i /Users/xiafan/快盘/dataset/exprresult/merge/raw -o /Users/xiafan/快盘/dataset/exprresult/merge/transform/ -k -t time -f basetree_time atomic_time segstore_time compute_score LIST_IO_time de_sketch other_cost -s 0 2 12 24  -p hpl_ns hpl_s ipl_s ipl_ns -x width -y k
