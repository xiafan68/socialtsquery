# encoding:utf8
from itertools import cycle
import itertools
import json
from optparse import OptionParser
import os
import re
from symbol import factor
import sys

import matplotlib as mpl
import matplotlib.pyplot as plt
from scipy.special.basic import h2vp
from matplotlib.pyplot import xticks


lines = ["-"]
markers = ["s", "o", "d", "p", "h", "x", "^", "v", "8"]

mpl.rcParams['lines.linewidth'] = 1
mpl.rcParams['lines.markersize'] = 8
mpl.rcParams['lines.color'] = 'black'
mpl.rcParams['font.family'] = 'Times-New-Roman'
mpl.rcParams['font.size'] = '20'

def cleanDirs(dirOrFile):
    # os.removedirs(dirOrFile)
    if os.path.isdir(dirOrFile):
        for file in os.listdir(dirOrFile):
            cleanDirs(os.path.join(dirOrFile, file))
        os.rmdir(dirOrFile)
    else:
        os.remove(dirOrFile)
     
# width, offset, k
# file factors
# line dimension
# factor
# repsondent
class LineDef(object):
    def __init__(self, fileFactors, lineFactors, factor, respondent, idx):
        self.fileFactors = fileFactors
        self.lineFactors = lineFactors
        self.factor = factor
        self.respondent = respondent
        self.idx = idx

class ExprPloter(object):
    def __init__(self, xLabel, yLabels):
        self.xLabel = xLabel
        self.yLabels = yLabels
        self.lineDefs = []
        self.dataMatrix = {}
        self.pattern = re.compile("part([0-9]+)(_.*)?")
        self.filePattern = re.compile("index_[^0-9]*([0-9]+).txt")
        self.needFiles = set(["k_50_offset_0", "width_12_k_50", "width_12_offset_0", "width_12_k_50_offset_0"])
    def addLines(self, lineDef):
        self.lineDefs.append(lineDef)
        
    @staticmethod
    def getFileFactors(curMap, factors):
        key = "_".join(factors)
        if not(key in curMap):
            curMap[key] = {}
        return curMap[key]
    
    @staticmethod
    def setupLineFactor(curMap, lineFactors):
        key = ",".join(lineFactors)
        if not(key in curMap):
            curMap[key] = []
        return curMap[key]
    
    @staticmethod
    def extractField(json, path, default):
        fields = path.split(".")
        curMap = json
        for field in fields:
            if not (field in curMap):
                curMap = default
                break
            else:
                curMap = curMap[field]
        if isinstance(curMap, dict):
           curMap = float(curMap['totalTime']) / float(curMap['count'])
        if 'insert_total' in path:
            curMap = curMap / (1000 * 60)
        elif 'width' in path:
            curMap = curMap / 2
        elif 'offset' in path:
            curMap = curMap / 2
        return curMap
     
    @staticmethod
    def extractMethod(data):
        if "lsmi" in data:
            curMethod = "LSMI"
        elif "intern" in data or "lsmo" in data:
            curMethod = "LSMO"
        elif "tpii" in data or "TPII" in data:
            curMethod = "TPII"
        elif "hybrid" in data:
            curMethod = "HYBRID"
        else:
            raise  NameError("no method name is found in %s" % (data))
        return curMethod
            
    """
    加载数据
    """
    def loadFiles(self, dir):
        fileNames = os.listdir(dir)
        for fileName in fileNames:
            if fileName.startswith("."):
                continue
            if os.path.isdir(os.path.join(dir, fileName)):
                self.loadFiles(os.path.join(dir, fileName))
            else:
                groups = self.pattern.match(os.path.basename(dir))
                size = 80
                if groups != None:
                    size = long(groups.group(1)) * 5
                else:
                    groups = self.pattern.match(fileName)
                    if groups != None:
                        size = long(groups.group(1)) * 5
                    
                limit = 5
                limitGroup = self.filePattern.match(fileName)
                if limitGroup:
                    limit = int(limitGroup.group(1))
                
                fd = open(os.path.join(dir, fileName), "r")
                curMethod = ExprPloter.extractMethod(fileName)
                
                i = 1
                for line in fd.readlines():
                    rec = json.loads(line)
                    rec["limit"] = limit
                    rec["size"] = size
                    rec["app"] = curMethod
                    # if rec['width'] != 24:
                    #    continue
                    self.extractLine(rec, i)
                    i = i + 1
                fd.close()
                
    def extractLine(self, rec, i):
        for (lineDef, idx) in zip(self.lineDefs, range(len(self.lineDefs))):
            factors = [factor + "_" + str(ExprPloter.extractField(rec, factor, lineDef.fileFactors[factor])) for factor in lineDef.fileFactors.keys()]
            fileMap = ExprPloter.getFileFactors(self.dataMatrix, factors) 
            lineFactors = [ExprPloter.extractField(rec, factor, lineDef.lineFactors[factor]) for factor in lineDef.lineFactors]
            # lineFactors.insert(0, curMethod)
            lineArr = ExprPloter.setupLineFactor(fileMap, lineFactors)
            lineArr.append({'factor':ExprPloter.extractField(rec, lineDef.factor, str(i * 10)), 'respondent':ExprPloter.extractField(rec, lineDef.respondent, ""), "lIdx":lineDef.idx}) 
       
    def plotFigures(self, outDir, config):
        scalex = config.get("scalex", None)
        scaley = config.get("scaley", None)
        ylim = config.get("ylim", None)
        leg = config.get("leg", None)
        figsize = config.get("figsize", None)
        
        if os.path.exists(outDir):
            # os.rmdir(outDir)
            cleanDirs(outDir)   

        os.makedirs(outDir)
        
        for k in self.dataMatrix.keys():
            if not(k in self.needFiles):
                continue;
            v = self.dataMatrix[k]
            if figsize:
                fig = plt.figure(figsize=figsize)
            else:
                fig = plt.figure()
            ax = fig.add_subplot(111)
            oaxIdx = None
            tax = None
            if len(self.yLabels) > 1:
                tax = ax.twinx()
            
            if ylim:
                if tax:
                    tax.set_ylim(ylim[0], ylim[1])
                else:
                    ax.set_ylim(ylim[0], ylim[1])
            # ax.set_xlim(0,1000)
            xscale_bias = 0
            if scalex:
                xscale_bias = 1
                ax.set_xscale('log')
            if scaley:
                ax.set_yscale('log')
                
            for (line, ltype, mtype) in zip(sorted(v.keys()), itertools.cycle(lines), itertools.cycle(markers)):
                tmpMap = {}
                lIdx = None
                for x in v[line]:
                    if (not x['factor'] in tmpMap):
                        tmpMap[x['factor']] = []
                    tmpMap[x['factor']].append(x['respondent']) 
                    lIdx = x['lIdx']
                if oaxIdx == None:
                    oaxIdx = lIdx
                 
                points = []
                for (factor, values) in sorted(tmpMap.items(), lambda x, y:cmp(int(x[0]), int(y[0]))):
                    sum = 0.0
                    for rec in values:
                        sum += rec
                    points.append({'factor':factor, 'respondent':sum / len(values)})
                
                
                    xline = [rec['factor'] + xscale_bias for rec in points ]  # for log scale
                yline = [rec['respondent'] for rec in points ]
                
                curAx = None
                if oaxIdx == lIdx:
                    curAx = ax
                else:
                    curAx = tax
                
                curAx.plot(xline, yline, "k" + ltype + mtype,
                          label=line, markerfacecolor="none", markeredgewidth=1, markersize=14)
                
                print ",".join(str(x) for x in xline)
                xlab = curAx.set_xlabel(self.xLabel)
                plt.setp(xlab, "fontsize", 22)
                ylab = curAx.set_ylabel(self.yLabels[lIdx])
                plt.setp(ylab, "fontsize", 22)
            # ax.set_ylim(0,1.1)
            ax.set(xticks=xline, xticklabels=[str(x - xscale_bias) for x in xline])
            # , ncol=2
            h1, l1 = ax.get_legend_handles_labels()
            if len(self.yLabels) > 1: 
                h2, l2 = tax.get_legend_handles_labels()
                h1 = h1 + h2
                l1 = l1 + l2
            ax.legend(h1, l1, loc=leg[0], framealpha=0.0, fontsize=22, ncol=leg[1], numpoints=1)
            
            # tax.legend(loc=[20,20], framealpha=0.0, fontsize=18, numpoints=1)
            fig.tight_layout()
            fig.savefig(os.path.join(outDir, str(k) + ".pdf"))
            

def plotScale(): 
    inputPath = "/Users/kc/快盘/dataset/weiboexpr/weibo_scale"
    outputDir = "/Users/kc/快盘/dataset/weiboexpr/weibo_scale_fig"
    
    inputPath = "/Users/kc/快盘/dataset/twitter_expr/twitter_scale"
    outputDir = "/Users/kc/快盘/dataset/twitter_expr/twitter_scale_fig"
    yrange = [0, 100]
    # inputPath = "/Users/kc/快盘/dataset/weiboexpr/scale_2"
    # outputDir = "/Users/kc/快盘/dataset/weiboexpr/scale_2_fig"
    # yrange=[0,450]
    
    ploter = ExprPloter("Percentage(%)", ["Average Latency(ms)"])
    ploter.addLines(LineDef({"offset":"offset", "width":"width", "k":"k"}, {"app":"app", "type":"type"}, "size", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    config = {"scalex":False, "scaley":False, "ylim":yrange, "leg":('upper left', 1)}
    ploter.plotFigures(os.path.join(outputDir, "size"), config)
    
def plotLimit():
    inputPath = "/Users/kc/快盘/dataset/weiboexpr/weibolimit"
    outputDir = "/Users/kc/快盘/dataset/weiboexpr/weibolimit_fig"
    inputPath = "/Users/kc/快盘/dataset/twitter_expr/twitterlimit_v3"
    outputDir = "/Users/kc/快盘/dataset/twitter_expr/twitterlimit_v3_fig"
    ploter = ExprPloter("Size Threshold($10^6$)", ["Latency(ms)"])
    ploter.addLines(LineDef({"offset":"offset", "width":"width", "k":"k", "size":"size"}, {"app":"app", "type":"type"}, "limit", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    config = {"scalex":True, "scaley":True, "ylim":[10, 100], "leg":('upper left', 3)}
    ploter.plotFigures(os.path.join(outputDir, "limit"), False, False, [15, 85])

def plotAll():   
    inputPath = "/Users/kc/快盘/dataset/twitter_expr/twitteresult/part16"
    outputDir = "/Users/kc/快盘/dataset/twitter_expr/part16_fig"
    
    inputPath = "/Users/kc/快盘/dataset/twitter_expr/result1/part20"
    outputDir = "/Users/kc/快盘/dataset/twitter_expr/result1/part20_fig"
    
    inputPath = "/Users/kc/Dropbox/数据/drawdata"
    outputDir = "/Users/kc/Dropbox/数据/drawfig"
    
    # inputPath = "/Users/kc/快盘/dataset/weiboexpr/scale_2/part20"
    # outputDir = "/Users/kc/快盘/dataset/weiboexpr/scale_2/part20_fig"
    ploter = ExprPloter("Offset(hour)", ["Latency(ms)"])
    ploter.addLines(LineDef({"width":"width", "k":"k"}, {"app":"app", "type":"type"}, "offset", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    # [0,110]
    # weibo [0,550]
    config = {"scalex":True, "scaley":True, "ylim":[10, 100], "leg":('upper left', 3)}
    ploter.plotFigures(os.path.join(outputDir, "offset"), config)
    
    ploter = ExprPloter("k", ["Latency(ms)"])
    ploter.addLines(LineDef({"width":"width", "offset":"offset"}, {"app":"app", "type":"type"}, "k", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    # [0,650]
    config = {"figsize":(15, 10), "scalex":False, "scaley":False, "ylim":[10, 100], "leg":('upper left', 3)}
    ploter.plotFigures(os.path.join(outputDir, "k"), config)
    
    ploter = ExprPloter("Query interval length(hour)", ["Latency(ms)"])
    ploter.addLines(LineDef({"k":"k", "offset":"offset"}, {"app":"app", "type":"type"}, "width", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    # [0,1500]
    config = {"scalex":True, "scaley":False, "ylim":[10, 700], "leg":('upper left', 2)}
    ploter.plotFigures(os.path.join(outputDir, "width"), config)
    
def plotThroughput():
    # 需要人为设置一下ncol=2
    inputPath = "/Users/kc/快盘/dataset/throughput/test2/twitter_throughput"
    outputDir = "/Users/kc/快盘/dataset/throughput/test2/twitter_throughput_fig"
    trange = [100, 300000000]
    # inputPath = "/Users/kc/快盘/dataset/throughput/test2/weibo_throughput"
    # outputDir = "/Users/kc/快盘/dataset/throughput/test2/weibo_throughput_fig"
    
    ploter = ExprPloter("Time(mins)", ["#Query"])
    ploter.addLines(LineDef({"":"line"}, {"app":"app", "query":"query"}, "time", "perf.query.count", 0))
    ploter.addLines(LineDef({"":"line"}, {"app":"app", "insert":"insert"}, "time", "perf.insert.count", 0))
    ploter.loadFiles(inputPath)
    config = {"scalex":False, "scaley":True, "ylim":trange, "leg":('upper left', 1)}
    ploter.plotFigures(os.path.join(outputDir, "throughput"), config)

def plotUpdateScale():
    inputPath = "/Users/kc/快盘/dataset/scale/twitter"
    outputDir = "/Users/kc/快盘/dataset/scale/twitter_fig"
    
    # inputPath = "/Users/kc/快盘/dataset/scale/weibo"
    # outputDir = "/Users/kc/快盘/dataset/scale/weibo_fig"
    
    ploter = ExprPloter("Percentage(%)", ["Total Latency(min)", "Average Latency(ms)"])
    ploter.addLines(LineDef({"":"line"}, {"app":"app", "insert total":"total"}, "size", "perf.insert_total.totalTime", 0))
    ploter.addLines(LineDef({"":"line"}, {"app":"app", "insert average":"average"}, "size", "perf.insert", 1))
    ploter.loadFiles(inputPath)
    config = {"scalex":False, "scaley":True, "ylim":[60, 150], "leg":('upper left', 1)}
    ploter.plotFigures(os.path.join(outputDir, "scale"), config)
    
if __name__ == "__main__":
    # "/Users/kc/快盘/dataset/weiboexpr/2015_12_03/raw"
    plotAll()
    # plotLimit()
    # plotScale()
    # plotUpdateScale()
    # plotThroughput()
   
    
