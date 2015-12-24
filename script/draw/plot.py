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
class ExprPloter(object):
    def __init__(self, fileFactors, lineFactors, factor, respondent):
        self.fileFactors = fileFactors
        self.lineFactors = lineFactors
        self.factor = factor
        self.respondent = respondent
        self.dataMatrix = {}
        self.pattern = re.compile("part([0-9]+)(_([-0-9]+))*")
    
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
                if groups == None:
                    return
                size = long(groups.group(1))
                fd = open(os.path.join(dir, fileName), "r")
                curMethod = ""
                if "lsmi" in fileName:
                    curMethod = "lsmi"
                else:
                    curMethod = "lsmo"
                
                for line in fd.readlines():
                    rec = json.loads(line)
                    #if rec['width'] != 24:
                    #    continue
                    rec["size"] = size
                    factors = [factor + "_" + str(rec[factor]) for factor in self.fileFactors]
                    fileMap = ExprPloter.getFileFactors(self.dataMatrix, factors) 
                    lineFactors = [rec[factor] for factor in self.lineFactors]
                    lineFactors.insert(0, curMethod)
                    lineArr = ExprPloter.setupLineFactor(fileMap, lineFactors)
                    lineArr.append({'factor':rec[self.factor], 'respondent':rec[self.respondent]})
                fd.close()
                
    def plotFigures(self, outDir, scalex):
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
            for (line, ltype, mtype) in zip(v.keys(), itertools.cycle(lines), itertools.cycle(markers)):
                tmpMap = {}
                for x in v[line]:
                    if (not x['factor'] in tmpMap):
                        tmpMap[x['factor']] = []
                    tmpMap[x['factor']].append(x['respondent'])  
                points = []
                for (factor, values) in sorted(tmpMap.items()):
                    sum = 0.0
                    for rec in values:
                        sum += rec
                    points.append({'factor':factor, 'respondent':sum / len(values)})
                xline = [rec['factor'] for rec in points ]
                yline = [rec['respondent'] for rec in points ]
                
                ax.plot(xline, yline, "k" + ltype + mtype,
                         label=line, markerfacecolor="none", markeredgewidth=1.5)
                xlab = ax.set_xlabel(self.factor)
                plt.setp(xlab, "fontsize", 18)
                ylab = ax.set_ylabel(self.respondent)
                plt.setp(ylab, "fontsize", 18)

            ax.legend(loc='best', framealpha=0.0, fontsize=22 * 0.7, numpoints=1)
            fig.savefig(os.path.join(outDir, str(k) + ".pdf"))
            

def plotScale():
    inputPath = "/Users/kc/Documents/dataset/weibo/expr/rawdata/"
    outputDir = "/Users/kc/Documents/dataset/weibo/expr/weibofigure_ubuntu/weibofigure_scale"
    ploter = ExprPloter(["offset", "width", "k"], ["type"], "size", "TOTAL_TIME")
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "size"), False)
    
def plotAll():
    inputPath = "/Users/kc/快盘/dataset/weiboexpr/expr/part20"
    outputDir = "/Users/kc/Documents/dataset/weibo/expr/weibofigure_ubuntu/weibofigure_20"
    
    ploter = ExprPloter(["width", "k"], ["type"], "offset", "TOTAL_TIME")
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "offset"), True)
    
    ploter = ExprPloter(["width", "offset"], ["type"], "k", "TOTAL_TIME")
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "k"), True)
    
    ploter = ExprPloter(["k", "offset"], ["type"], "width", "TOTAL_TIME")
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "width"), True)
if __name__ == "__main__":
    # "/Users/kc/快盘/dataset/weiboexpr/2015_12_03/raw"
    plotAll()
    #plotScale()
    
