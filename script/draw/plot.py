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
    def __init__(self, fileFactors, lineFactors, factor, respondent):
        self.fileFactors = fileFactors
        self.lineFactors = lineFactors
        self.factor = factor
        self.respondent = respondent
        

class ExprPloter(object):
    def __init__(self, xLabel, yLabel):
        self.xLabel = xLabel
        self.yLabel = yLabel
        self.lineDefs = []
        self.dataMatrix = {}
        self.pattern = re.compile("part([0-9]+)(_.*)?")
        self.filePattern = re.compile("index_[^0-9]*([0-9]+).txt")
    
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
        return curMap
     
    @staticmethod
    def extractMethod(data):
        if "lsmi" in data:
            curMethod = "lsmi"
        elif "intern" in data or "lsmo" in data:
            curMethod = "lsmo"
        elif "hybrid" in data:
            curMethod = "hybrid"
        else:
            raise  NameError("no hybrid name is found in %s" % (data))
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
                    rec["type"] = curMethod
                    # if rec['width'] != 24:
                    #    continue
                    self.extractLine(rec, i)
                    i = i + 1
                fd.close()
                
    def extractLine(self, rec, i):
        for lineDef in self.lineDefs:
            factors = [factor + "_" + str(ExprPloter.extractField(rec, factor, lineDef.fileFactors[factor])) for factor in lineDef.fileFactors.keys()]
            fileMap = ExprPloter.getFileFactors(self.dataMatrix, factors) 
            lineFactors = [ExprPloter.extractField(rec, factor, lineDef.lineFactors[factor]) for factor in lineDef.lineFactors]
            # lineFactors.insert(0, curMethod)
            lineArr = ExprPloter.setupLineFactor(fileMap, lineFactors)
            lineArr.append({'factor':ExprPloter.extractField(rec, lineDef.factor, str(i * 10)), 'respondent':ExprPloter.extractField(rec, lineDef.respondent, "")}) 
       
    def plotFigures(self, outDir, scalex, scaley):
        if os.path.exists(outDir):
            # os.rmdir(outDir)
            cleanDirs(outDir)   

        os.makedirs(outDir)
        
        for k in self.dataMatrix.keys():
            v = self.dataMatrix[k]
            fig = plt.figure()
            ax = fig.add_subplot(111)
            if scalex:
                ax.set_xscale('log')
            if scaley:
                ax.set_yscale('log')
                
            for (line, ltype, mtype) in zip(sorted(v.keys()), itertools.cycle(lines), itertools.cycle(markers)):
                tmpMap = {}
                for x in v[line]:
                    if (not x['factor'] in tmpMap):
                        tmpMap[x['factor']] = []
                    tmpMap[x['factor']].append(x['respondent'])  
                points = []
                for (factor, values) in sorted(tmpMap.items(), lambda x, y:cmp(int(x[0]), int(y[0]))):
                    sum = 0.0
                    for rec in values:
                        sum += rec
                    points.append({'factor':factor, 'respondent':sum / len(values)})
                xline = [rec['factor'] for rec in points ]
                yline = [rec['respondent'] for rec in points ]
                
                ax.plot(xline, yline, "k" + ltype + mtype,
                         label=line, markerfacecolor="none", markeredgewidth=1.5)
                xlab = ax.set_xlabel(self.xLabel)
                plt.setp(xlab, "fontsize", 18)
                ylab = ax.set_ylabel(self.yLabel)
                plt.setp(ylab, "fontsize", 18)

            ax.legend(loc='best', framealpha=0.0, fontsize=22 * 0.7, numpoints=1)
            fig.savefig(os.path.join(outDir, str(k) + ".pdf"))
            

def plotScale():
    inputPath = "/Users/kc/Documents/dataset/weibo/expr/rawdata/"
    inputPath = "/Users/kc/快盘/dataset/weiboexpr/weiboscale"
    outputDir = "/Users/kc/Documents/dataset/weibo/expr/weibofigure_ubuntu/weibofigure_scale"
    outputDir = "/Users/kc/快盘/dataset/weiboexpr/weiboscale_fig"
    ploter = ExprPloter(["offset", "width", "k"], ["type"], "size(%)", "TOTAL_TIME")
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "size"), False)
    
def plotLimit():
    inputPath = "/Users/kc/快盘/dataset/weiboexpr/weibolimit"
    outputDir = "/Users/kc/快盘/dataset/weiboexpr/weibolimit_fig"
    ploter = ExprPloter(["offset", "width", "k", "size(%)"], ["type"], "limit", "TOTAL_TIME")
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "limit"), False)
    
def plotAll():
    inputPath = "/Users/kc/快盘/dataset/weiboexpr/expr/part20"
    inputPath = "/Users/kc/快盘/dataset/twitter_expr/twitteresult/part12"
    inputPath = "/Users/kc/快盘/dataset/twitter_expr/twitteresult/part16"

    inputPath = "/Users/kc/快盘/dataset/weiboexpr/weibofacts"
    outputDir = "/Users/kc/快盘/dataset/weiboexpr/weibofacts_fig"
    
    ploter = ExprPloter(["width", "k"], ["type"], "offset", "TOTAL_TIME")
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "offset"), False)
    
    ploter = ExprPloter(["width", "offset"], ["type"], "k", "TOTAL_TIME")
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "k"), True)
    
    ploter = ExprPloter(["k", "offset"], ["type"], "width", "TOTAL_TIME")
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "width"), True)
    
def plotThroughput():
    inputPath = "/Users/kc/快盘/dataset/throughput/twitter_throughput"
    outputDir = "/Users/kc/快盘/dataset/throughput/twitter_throughput_fig"
    
    inputPath = "/Users/kc/快盘/dataset/throughput/weibo_throughput"
    outputDir = "/Users/kc/快盘/dataset/throughput/weibo_throughput_fig"
    
    ploter = ExprPloter("Time Elapsed(mins)", "#query")
    ploter.addLines(LineDef({"":"line"}, {"type":"type", "query":"query"}, "time", "perf.query.count"))
    ploter.addLines(LineDef({"":"line"}, {"type":"type", "insert":"insert"}, "time", "perf.insert.count"))
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "throughput"), False, True)

def plotUpdateScale():
    inputPath = "/Users/kc/快盘/dataset/scale/twitter"
    outputDir = "/Users/kc/快盘/dataset/scale/twitter_fig"
    
    # inputPath = "/Users/kc/快盘/dataset/scale/weibo"
    # outputDir = "/Users/kc/快盘/dataset/scale/weibo_fig"
    
    ploter = ExprPloter("size", "Time Elapse(ms)")
    ploter.addLines(LineDef({"":"line"}, {"type":"type", "insert total":"insert total"}, "size", "perf.insert_total.totalTime"))
    ploter.addLines(LineDef({"":"line"}, {"type":"type", "insert average":"insert average"}, "size", "perf.insert"))
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "scale"), False, True)
    
if __name__ == "__main__":
    # "/Users/kc/快盘/dataset/weiboexpr/2015_12_03/raw"
    # plotAll()
    # plotScale()
    # plotLimit()
    #plotThroughput()
    plotUpdateScale()
    
