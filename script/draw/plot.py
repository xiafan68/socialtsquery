# encoding:utf8
import sys
import os
import json
from optparse import OptionParser
import matplotlib.pyplot as plt
from symbol import factor
from itertools import cycle
import itertools
import matplotlib as mpl

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
            curMap[key] = {}
            curMap[key]["factor"] = []
            curMap[key]["respondent"] = []       
        return curMap[key]
        
    """
    加载数据
    """
    def loadFiles(self, dir):
        fileNames = os.listdir(dir)
        for fileName in fileNames:
            fd = open(os.path.join(dir, fileName), "r")
            curMethod = ""
            if "lsmi" in fileName:
                curMethod = "lsmi"
            else:
                curMethod = "lsmo"
            
            for line in fd.readlines():
                rec = json.loads(line)
                factors = [factor + "_" + str(rec[factor]) for factor in self.fileFactors]
                fileMap = ExprPloter.getFileFactors(self.dataMatrix, factors) 
                lineFactors = [rec[factor] for factor in self.lineFactors]
                lineFactors.insert(0, curMethod)
                lineMap = ExprPloter.setupLineFactor(fileMap, lineFactors)
                lineMap['factor'].append(rec[self.factor])
                lineMap['respondent'].append(rec[self.respondent])
            fd.close()
                
    def plotFigures(self, outDir):
        if os.path.exists(outDir):
            # os.rmdir(outDir)
            cleanDirs(outDir)   
        
        os.mkdir(outDir)
        
        for k in self.dataMatrix.keys():
            v = self.dataMatrix[k]
            fig = plt.figure()
            ax = fig.add_subplot(111)
            ax.set_xscale('log')
            for (line, ltype, mtype) in zip(v.keys(), itertools.cycle(lines), itertools.cycle(markers)):
                lineData = v[line]
                ax.plot(lineData['factor'], lineData["respondent"], "k" + ltype + mtype,
                         label=line, markerfacecolor="none", markeredgewidth=1.5)
                xlab = ax.set_xlabel(self.factor)
                plt.setp(xlab, "fontsize", 18)
                ylab = ax.set_ylabel(self.respondent)
                plt.setp(ylab, "fontsize", 18)
            ax.legend(loc='best', framealpha=0.0, fontsize=22 * 0.7, numpoints=1)
            fig.savefig(os.path.join(outDir, str(k) + ".pdf"))
            
    
if __name__ == "__main__":
    ploter = ExprPloter(["width", "k"], ["type"], "offset", "TOTAL_TIME")
    ploter.loadFiles("/Users/kc/快盘/dataset/weiboexpr/2015_12_03/raw")
    ploter.plotFigures("/Users/kc/Documents/dataset/weibo/expr/weibofigure/offset")
    
    ploter = ExprPloter(["width", "offset"], ["type"], "k", "TOTAL_TIME")
    ploter.loadFiles("/Users/kc/快盘/dataset/weiboexpr/2015_12_03/raw")
    ploter.plotFigures("/Users/kc/Documents/dataset/weibo/expr/weibofigure/k")
    
    ploter = ExprPloter(["k", "offset"], ["type"], "width", "TOTAL_TIME")
    ploter.loadFiles("/Users/kc/快盘/dataset/weiboexpr/2015_12_03/raw")
    ploter.plotFigures("/Users/kc/Documents/dataset/weibo/expr/weibofigure/width")

