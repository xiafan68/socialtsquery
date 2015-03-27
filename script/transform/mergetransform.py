# encoding:utf8
import sys
import os
import json
from optparse import OptionParser

class Transformer(object):
    def __init__(self, idir, prefixs, start):
        self.idir = idir.decode("utf8")
        self.prefixs = prefixs
        self.start = start

    def loadRecs(self, xaxis, yaxis):
        self.xaxis = xaxis
        self.yaxis = yaxis
        self.recs={}
        self.xval = set()
        self.yval = set()
        for prefix in self.prefixs:
            input = open(os.path.join(self.idir,prefix+"_o"+self.start+"w.txt"), "r")
            
            for line in input.readlines():
                row = json.loads(line)
                if not(row[self.xaxis] in self.recs):
                    self.recs[row[self.xaxis]]={}
                curDict = self.recs[row[self.xaxis]]
                if not(prefix in curDict):
                    curDict[prefix]={}
                curDict = curDict[prefix]
                curDict[row[self.yaxis]] = row
                
                self.xval.add(row[self.xaxis])
                self.yval.add(row[self.yaxis])
            input.close()
        self.xval = sorted(self.xval)
        self.yval = sorted(self.yval)
    """
    xaxis    approaches yaxis[1]    ....
                    field1 field2 field3
    """
    def writeHeader(self, output, fields):
        header = "axis\tapp"
        stubs = "\t".join([" " for i in range(len(fields) - 1)])
        for yval in self.yval:
            header = "\t%s\t%s\t%s"%(header,str(yval), stubs)

        output.write(header+"\n")
        header = " \t \t"
        fieldsStr = "\t".join(fields)
        fieldArr=[]
        for i in range(len(self.yval)):
            fieldArr.append(fieldsStr)
        header += "\t".join(fieldArr)
        output.write(header + "\n")
        
    def transform(self, oFile, fields):
        matrix = {}
        output = open(oFile, "w")
        self.writeHeader(output, fields)
        
        for xval in self.xval:
            xLine=str(xval)
            for prefix in self.prefixs:
                prefixLine = "%s\t%s"%(xLine,prefix.replace("_","\\\\_"))
                line = prefixLine
                rec = self.recs[xval][prefix]
                for yval in self.yval:
                    yrec = rec[yval]
                    line = "%s\t%s"%(line, "\t".join([str(yrec[field]) for field in fields]))
                output.write(line)
                output.write("\n")
        output.close()
        
    def transformTime(self, oFile, fields):
        matrix = {}
        output = open(oFile, "w")
        self.writeHeader(output, fields)
        
        for xval in self.xval:
            xLine=str(xval)
            for prefix in self.prefixs:
                prefixLine = "%s\t%s"%(xLine,prefix)
                line = prefixLine
                rec = self.recs[xval][prefix]
                for yval in self.yval:
                    yrec = rec[yval]
                    newRec = []
                    agg=0.0
                    for field in fields:
                        if field != 'total_time':
                            agg += yrec[field]
                            newRec.append(yrec[field])
                    newRec.append(yrec['total_time'] - agg)
                    line = "%s\t%s"%(line, "\t".join([str(field) for field in newRec]))
                output.write(line)
                output.write("\n")
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

"""
将多个文件中的数据进行合并，得到如下格式：
xaxis    approaches yaxis[1]    ....
                    field1 field2 field3
"""
if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-i", "--idir",dest="inputs", help="input directory")
    parser.add_option("-p", "--prefix",action="callback",callback=vararg_callback, dest="prefix", help="prefix of input files")
    parser.add_option("-s", "--start", dest="start", help="offset from the start time of event")
    parser.add_option("-o", "--odir", dest="odir", help="output directory")
    parser.add_option("-x", "--xaxis", dest="xaxis", help="x axis")
    parser.add_option("-y", "--yaxis", dest="yaxis", help="y axis")
    parser.add_option("-f", "--fields", dest="fields",action="callback",callback=vararg_callback, help="fields")
    parser.add_option("-t", "--tag", dest="tag",help="tag that will appended to the output file name")
    parser.add_option("-m", "--minus", dest="minus",action="store_true",default=False, help="whether minus other costs from the total time" )
    
    (options, args) = parser.parse_args(sys.argv)
    print args
    if len(args) != 1:
        parser.print_help()
    print "input file",options.prefix
    t = Transformer(options.inputs, options.prefix, options.start)
    t.loadRecs(options.xaxis, options.yaxis)
    #oFile = os.path.join(options.odir, "k_w", "IO")
    if not os.path.exists(options.odir):
        os.makedirs(options.odir)
    tag = ""
    if options.tag:
        tag = options.tag
    fName = "%s_%s_s%s_%s.txt"%(options.xaxis,options.yaxis,options.start, tag)
    oFile = os.path.join(options.odir, fName)
    
    t.transform(oFile, options.fields)

 
   #t.transform(oFile, "width", "k", ["atomic", "segstore", "basetree", "LIST_IO"])
#-p  hpl_ns hpl_s ipl_s ipl_ns -i  /home/xiafan/KuaiPan/dataset/exprresult/2014_12_26/raw -s 0 -o  /home/xiafan/KuaiPan/dataset/exprresult/2014_12_26/transform -f basetree atomic segstore LIST_IO  -x k -y width -t io
#-p  hpl_ns hpl_s ipl_s ipl_ns -i  /home/xiafan/KuaiPan/dataset/exprresult/2014_12_26/raw -s 0 -o  /home/xiafan/KuaiPan/dataset/exprresult/2014_12_26/transform -f basetree_time atomic_time segstore_time compute_score LIST_IO_time de_sketch other_cost  -x k -y width -t time
