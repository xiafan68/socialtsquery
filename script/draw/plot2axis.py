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
from matplotlib.pyplot import xticks
from matplotlib.font_manager import path
from array import array
'''
设置figure的顶部和底部位置，以便为边界留白：https://matplotlib.org/api/_as_gen/matplotlib.pyplot.subplots_adjust.html
set_bbox_to_anchor函数，用于设置legend的位置：https://github.com/matplotlib/matplotlib/blob/master/lib/matplotlib/legend.py#L112
'''
lines = ["-"]
markers = ["s", "x", "|", "v", ">","d", "h", "v", "^", ">"]
dataDir="/Users/kc/快盘/"
dataDir="/home/xiafan/Dropbox"
dataDir="/Volumes/backupsd/Dropbox"

outDir="/Users/kc/Documents/temp/pic_and"

def smallFigSetups():
    mpl.rcParams['lines.linewidth'] = 2
    mpl.rcParams['lines.markersize'] = 18
    mpl.rcParams['lines.color'] = 'black'
    mpl.rcParams['font.family'] = 'Times-New-Roman'
    mpl.rcParams['font.size'] = '28'
    mpl.rcParams['legend.columnspacing']= 1

def largeFigureSetups():
    mpl.rcParams['lines.linewidth'] = 1
    mpl.rcParams['lines.markersize'] = 8
    mpl.rcParams['lines.color'] = 'black'
    mpl.rcParams['font.family'] = 'Times-New-Roman'
    mpl.rcParams['font.size'] = '24'
    

smallFigSetups()

WEIGHTED = "WOR"
WEIGHTED_IN_FILE="WEIGHTED"

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
    def __init__(self, xLabel, yLabels, semantics = set(["OR", "WEIGHTED"])):
        self.semantics = semantics
        self.xLabel = xLabel
        self.yLabels = yLabels
        self.lineDefs = []
        self.dataMatrix = {}
        self.pattern = re.compile(".*part([0-9]+)(_.*)?")
        self.filePattern = re.compile("index_.*(weibo|twitter)([0-9]+).txt")
        self.needFiles = set(["k_50_size_80_offset_0",
                              "width_12_k_50_size_80",
                              "width_12_size_80_offset_0", "width_12_k_50_offset_0_size_80", "width_12_k_50_offset_0",
                              "width_12_k_50_size_80_offset_0", "_line","width_12_k_10_size_100_offset_0"])
    def addLines(self, lineDef):
        self.lineDefs.append(lineDef)

    @staticmethod
    def getFileFactors(curMap, factors):
        key = "_".join(factors)
        if not(key in curMap):
            curMap[key] = {}
        return curMap[key]

    """
    计算每跟线的legend标题，例如lsmi, weighted
    """
    @staticmethod
    def setupLineFactor(curMap, lineFactors):
        key = ",".join(lineFactors)
        key=key.replace(WEIGHTED_IN_FILE, WEIGHTED)
        if not(key in curMap):
            curMap[key] = []
        return curMap[key]

    """
    从json中抽取出path表示的路径对应的值，如果不存在就返回default
    """
    @staticmethod
    def extractSingleField(json, path, default):
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
    def extractField(json, path, default):
        try:
            if not(isinstance(path, list)):
                return ExprPloter.extractSingleField(json, path, default)
            else:
                ret = 0.0
                for aPath in path:
                    ret += float(ExprPloter.extractSingleField(json, aPath, default))
                return ret
        except Exception, ex:
            print json,path,str(ex)
    @staticmethod
    def extractMethod(data):
        if "lsmi" in data:
            curMethod = "LSMI"
        elif "lsmo_bdb" in data:
            curMethod = "bdb"
        elif "intern" in data or "lsmo" in data:
            curMethod = "LSMO"
        elif "tpii" in data or "TPII" in data:
            curMethod = "TPII"
        elif "hybrid" in data:
            curMethod = "HYBRID"
        elif "lucene" in data:
            curMethod = "LUCENE"
        elif "octree" in data:
            curMethod = "octree"
        elif "hpl" in data:
            curMethod = "HPL"
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
                    limit = int(limitGroup.group(2))

                fd = open(os.path.join(dir, fileName), "r")
                curMethod = ExprPloter.extractMethod(fileName)
                
                if curMethod == "HYBRID" or curMethod == "bdb" or curMethod == "octree":
                    continue
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
            skip = False
            for semantic in lineFactors:
                if semantic in self.semantics:
                    skip = True
            if skip:
                continue
            # lineFactors.insert(0, curMethod)
            lineArr = ExprPloter.setupLineFactor(fileMap, lineFactors)
            lineArr.append({'factor':ExprPloter.extractField(rec, lineDef.factor, str(i * 10)), 'respondent':ExprPloter.extractField(rec, lineDef.respondent, ""), "lIdx":lineDef.idx})

    def plotFigures(self, outDir, config):
        scalex = config.get("scalex", None)
        scaley = config.get("scaley", None)
        ylim = config.get("ylim", None)
        leg = config.get("leg", None)
        figsize = config.get("figsize", None)
        legsize = config.get("legsize", 20)
        xscale_bias = config.get("xscale_bias", 0)
        round_x=config.get("round_x", False)
        trun_start=config.get("trun_start", False)
        bbox_to_anchor=config.get("bbox_to_anchor", None)
        
        if os.path.exists(outDir):
            # os.rmdir(outDir)
            cleanDirs(outDir)

        os.makedirs(outDir)

        for k in self.dataMatrix.keys():
            if not(k in self.needFiles):
                continue
            """
            found = False;
            for line in self.needFiles:
                if line in k:
                    found = True
                    break
            if not found:
                continue
            """
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
                ax.set_ylim(ylim[0], ylim[1])
            if "ylim1" in config:
                ylim1 = config.get("ylim1")
                tax.set_ylim(ylim1[0], ylim1[1])
                    
            # ax.set_xlim(0,1000)
            if scalex:
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
                    count = 0
                    for rec in values:
                        if not isinstance(rec, str):
                            sum += rec
                            count = count + 1
                    points.append({'factor':factor, 'respondent':sum / count})


                xline = [int(rec['factor']) + xscale_bias for rec in points ]  # for log scale
                yline = [rec['respondent'] for rec in points ]

                curAx = None
                if oaxIdx == lIdx:
                    curAx = ax
                else:
                    curAx = tax
                label = line
                if config.get("without_semantic", True) and label.endswith("AND"):
                    label = label[0:label.find(",")]
                curAx.plot(xline, yline, "k" + ltype + mtype,
                          label=label, markerfacecolor="none", markeredgewidth=4, markersize=40)

                print ",".join(str(x) for x in xline)
                xlab = curAx.set_xlabel(self.xLabel, fontsize=config.get("labelSize", 40))
                ylab = curAx.set_ylabel(self.yLabels[lIdx], fontsize=config.get("labelSize", 40))
                curAx.tick_params(labelsize=config.get("labelSize", 40))
            if round_x:
                xlim = range(0, (int(xline[-1]) + 49) / 50 * 50 + 1, 50)
                plt.xticks(xlim, rotation=config.get("rotation", "horizontal"), fontsize=config.get("xSize", 36))
            elif trun_start:
                plt.xticks(xline[2:], rotation=config.get("rotation", "horizontal"), fontsize=config.get("xSize", 36))
            else:
                plt.xticks(xline, [str(x - xscale_bias) for x in xline], rotation=config.get("rotation", "horizontal"), 
                           fontsize=config.get("xSize", 36))
            
            h1, l1 = ax.get_legend_handles_labels()
            if len(self.yLabels) > 1:
                h2, l2 = tax.get_legend_handles_labels()
                h1 = h1 + h2
                l1 = l1 + l2
            legend = ax.legend(h1, l1, loc=leg[0], framealpha=config.get("framealpha", 0.0), 
                               columnspacing=config.get("columnspacing",None),
                               fontsize=legsize, ncol=leg[1], numpoints=1)
            if bbox_to_anchor:
                legend.set_bbox_to_anchor(bbox_to_anchor)
                fig.subplots_adjust(left = config.get("subplot.left", 0.2), right=config.get("subplot.right", 1.0), bottom=config.get("subplot.bottom",0.0), top=config.get("subplot.top", 0.79))
            else:
                fig.tight_layout()
           
            # tax.legend(loc=[20,20], framealpha=0.0, fontsize=18, numpoints=1)
            suffix = config.get("suffix", "")
            fig.savefig(os.path.join(outDir, str(k) + suffix + ".pdf"))


def plotScaleForWeibo():
    inputPath = dataDir+"/数据/weibo_result/weibo_50_hasresult"
    outputDir = outDir+"/weibo_result/weibo_50_hasresult_scala_fig"
    
    yrange = [1, 200000]
    ploter = ExprPloter("Percentage (%)", ["Latency (ms)"])
    ploter.addLines(LineDef({"offset":"offset", "width":"width", "k":"k"}, {"app":"app", "type":"type"}, "size", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    config = {"bbox_to_anchor": (0, 1),"subplot.bottom": 0.15,"subplot.top":0.98,"subplot.right":0.94, 
              "scalex":False, "figsize":(9, 9), "scaley":True, "ylim":yrange, "leg":('upper left', 2), "legsize":32}
    ploter.plotFigures(os.path.join(outputDir, "size"), config)

def plotScaleForTwitter():
    inputPath = dataDir+"/数据/twitter_result/twitter_50_hasresult"
    outputDir = outDir+"/twitter_result/twitter_50_hasresult_scale_fig"
    
    ploter = ExprPloter("Percentage (%)", ["Latency (ms)"])
    ploter.addLines(LineDef({"offset":"offset", "width":"width", "k":"k"}, {"app":"app", "type":"type"}, "size", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    config = {"bbox_to_anchor": (0, 1), "subplot.bottom": 0.15,"subplot.top":0.98,"subplot.right":0.94, 
              "scalex":False, "figsize":(9, 9), "scaley":True, "ylim":[1, 50000], 
              "leg":('upper left', 2), "legsize":32}
    ploter.plotFigures(os.path.join(outputDir, "size"), config)

def plotLimitForWeibo():
    inputPath = dataDir+"dataset/weiboexpr/weibolimit"
    outputDir = dataDir+"dataset/weiboexpr/weibolimit_fig"
    inputPath = "/home/xiafan/expr/tao/weibo"
    outputDir = "/home/xiafan/expr/tao/weibo_fig"
    inputPath = dataDir+"/数据/weibo_result/weibolimit"
    outputDir = outDir+"/limit_50/weibo_fig"
    ploter = ExprPloter(r"$\tau$ $(10^6)$", ["Latency (ms)"], set())
    ploter.addLines(LineDef({"offset":"offset", "width":"width", "k":"k", "size":"size"}, {"app":"app", "type":"type"}, "limit", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    config = {"bbox_to_anchor": (0.43, 1.52), "subplot.left":0.19,"subplot.top":0.72, "subplot.bottom": 0.14, 
              "framealpha":1.0, "columnspacing":1.2, "without_semantic":False,"figsize":(9,9),
              "scalex":False, "scaley":False, "ylim":[1, 350], "leg":('upper center', 2), "legsize":32}
    ploter.plotFigures(os.path.join(outputDir, "limit"), config)

def plotLimitForTwitter():
    inputPath = dataDir+"dataset/twitter_expr/twitterlimit_v3"
    outputDir = dataDir+"dataset/twitter_expr/twitterlimit_v3_fig"
    inputPath = "/home/xiafan/expr/limit_50/twitter"
    inputPath=dataDir+"/数据/limit_50/twitter"
    outputDir = outDir+"/limit_50/twitter_fig"
    ploter = ExprPloter(r"$\tau$ $(10^6)$", ["Latency (ms)"], set())
    ploter.addLines(LineDef({"offset":"offset", "width":"width", "k":"k", "size":"size"}, {"app":"app", "type":"type"}, "limit", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    config = {"bbox_to_anchor": (0.44, 1.5), "subplot.left":0.15,"subplot.top":0.72, "subplot.bottom": 0.14, 
              "framealpha":1.0, "columnspacing":1.2,
              "figsize":(9,9),"without_semantic":False,
              "scalex":False, "scaley":False, "ylim":[1, 90], "leg":('upper center', 2), "legsize":32}
    ploter.plotFigures(os.path.join(outputDir, "limit"), config)
    
def plotAllForWeibo():
    inputPath = dataDir+"/数据/weibo_result/weibo_50_hasresult"
    outputDir = outDir+"/weibo_result/weibo_50_hasresult_fact_fig"
    # offset
    ploter = ExprPloter("Deviation (hour)", ["Latency (ms)"])
    ploter.addLines(LineDef({"width":"width", "k":"k", "size":"size"}, {"app":"app", "type":"type"}, "offset", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    config = {"bbox_to_anchor": (0.5, 1),"subplot.left":0.22,"subplot.right":0.97,"subplot.bottom":0.15,"subplot.top":0.98,
              "xscale_bias":1, "figsize":(9, 9),"scalex":True, "scaley":True, "ylim":[1, 500000], "leg":('upper center', 2), "legsize":32}
    ploter.plotFigures(os.path.join(outputDir, "offset"), config)

    # k
    ploter = ExprPloter("Q.k", ["Latency (ms)"])
    ploter.addLines(LineDef({"width":"width", "offset":"offset", "size":"size"}, {"app":"app", "type":"type"}, "k", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    # "figsize":(15, 10), 
    #[10,500]
    config = {"bbox_to_anchor": (0.5, 1),"subplot.left":0.22,"subplot.right":0.96,"subplot.bottom":0.15,"subplot.top":0.98,
              "xSize": 28, "trun_start":True, "figsize":(9, 9), "scalex":False, "scaley":True, 
              "ylim":[10, 500000], "leg":('upper center', 2), "legsize":32}
    ploter.plotFigures(os.path.join(outputDir, "k"), config)

    # query width
    #200000
    config = {"bbox_to_anchor": (0.0, 1),"subplot.left":0.22,"subplot.right":0.97,"subplot.bottom":0.15,"subplot.top":0.98,
              "scalex":True, "figsize":(9, 9), "scaley":True, "ylim":[10, 200000], "leg":('upper left', 2), "legsize":32}
    ploter = ExprPloter("|Q.I| (hour)", ["Latency (ms)"])
    ploter.addLines(LineDef({"k":"k", "offset":"offset", "size":"size"}, {"app":"app", "type":"type"}, "width", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "width"), config)

    #-----------disk io-------------
    '''
    config = {"figsize":(9, 7), "scalex":False, "scaley":False, "ylim":[1, 250], "leg":('upper left', 2), "suffix":"_io", "legsize":20}
    ploter = ExprPloter("k", ["Disk blocks(4kb)"])
    ploter.addLines(LineDef({"width":"width", "offset":"offset", "size":"size"}, {"app":"app", "type":"type"}, "k", "READ_BLOCK", 0))
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "io"), config)

    config = {"figsize":(15, 10), "scalex":False, "scaley":False, "ylim":[1, 30000], "leg":('upper left', 3), "suffix":"_rec", "legsize":20}
    ploter = ExprPloter("k", ["Number of useless segments"])
    ploter.addLines(LineDef({"width":"width", "offset":"offset", "size":"size"}, {"app":"app", "type":"type"}, "k", ["WASTED_REC", "CAND"], 0))
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "recs"), config)
    '''

def plotAllForTwitter():
    inputPath = dataDir+"/数据/twitter_result/twitter_50_hasresult"
    outputDir = outDir+"/twitter_50_hasresult_fig"
    # offset
    ploter = ExprPloter("Deviation (hour)", ["Latency (ms)"])
    ploter.addLines(LineDef({"width":"width", "k":"k", "size":"size"}, {"app":"app", "type":"type"}, "offset", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    config = {"bbox_to_anchor": (1, 1),"subplot.left":0.22,"subplot.right":0.97,"subplot.bottom":0.15,"subplot.top":0.98,
              "xscale_bias":1, "figsize":(9, 9),"scalex":True, "scaley":True, "ylim":[1, 400000], "leg":('upper right', 2), "legsize":32}
    ploter.plotFigures(os.path.join(outputDir, "offset"), config)

    # k
    ploter = ExprPloter("Q.k", ["Latency (ms)"])
    ploter.addLines(LineDef({"width":"width", "offset":"offset", "size":"size"}, {"app":"app", "type":"type"}, "k", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    config = {"bbox_to_anchor": (1, 1),"subplot.left":0.22,"subplot.right":0.96,"subplot.bottom":0.15,"subplot.top":0.98,
              "xSize": 28, "trun_start":True, "figsize":(9, 9), "scalex":False, "scaley":True,
               "ylim":[1, 500000], "leg":('upper right', 2), "legsize":32}
    ploter.plotFigures(os.path.join(outputDir, "k"), config)

    # query width
    config = {"bbox_to_anchor": (0.5, 1),"subplot.left":0.22,"subplot.right":0.97,"subplot.bottom":0.15,"subplot.top":0.98,
              "scalex":True, "figsize":(9, 9), "scaley":True, "ylim":[1, 400000], "leg":('upper center', 2), "legsize":32}
    ploter = ExprPloter("|Q.I| (hour)", ["Latency (ms)"])
    ploter.addLines(LineDef({"k":"k", "offset":"offset", "size":"size"}, {"app":"app", "type":"type"}, "width", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "width"), config)

    #-----------disk io-------------
    '''
    config = {"figsize":(15, 10), "scalex":False, "scaley":False, "ylim":[1, 55], "leg":('upper left', 3), "suffix":"_io", "legsize":32}
    ploter = ExprPloter("k", ["Disk blocks (4kB)"])
    ploter.addLines(LineDef({"width":"width", "offset":"offset", "size":"size"}, {"app":"app", "type":"type"}, "k", "READ_BLOCK", 0))
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "io"), config)

    config = {"figsize":(15, 10), "scalex":False, "scaley":False, "ylim":[1, 30000], "leg":('upper left', 3), "suffix":"_rec", "legsize":20}
    ploter = ExprPloter("k", ["Number of useless segments"])
    ploter.addLines(LineDef({"width":"width", "offset":"offset", "size":"size"}, {"app":"app", "type":"type"}, "k", ["WASTED_REC", "CAND"], 0))
    ploter.loadFiles(inputPath)
    ploter.plotFigures(os.path.join(outputDir, "recs"), config)
    '''
def plotThroughputForWeibo():
    # 需要人为设置一下ncol=2
    trange = [100, 10000000]
    inputPath = dataDir+"/数据/throughput/weibo_throughput"
    outputDir = outDir+"/weibo_throughput_fig"

    ploter = ExprPloter("Time (min)", ["#queries / #insertions"], set())
    ploter.addLines(LineDef({"":"line"}, {"app":"app", "query":"query"}, "time", "perf.query.count", 0))
    ploter.addLines(LineDef({"":"line"}, {"app":"app", "insert":"insert"}, "time", "perf.insert.count", 0))
    ploter.loadFiles(inputPath)
    config = {"bbox_to_anchor": (0.4, 1.19), "subplot.right":0.96,"subplot.top":0.78, "subplot.bottom": 0.1, "framealpha":1.0, 
              "columnspacing":1.2,"legsize":32,
              "figsize":(9,9),"round_x":True, "scalex":False, "scaley":True, "ylim":trange, "leg":('center', 2), "xSize":18}
    ploter.plotFigures(os.path.join(outputDir, "throughput"), config)

def plotThroughputForTwitter():
    # 需要人为设置一下ncol=2
    inputPath = dataDir+"/数据/throughput/twitter_throughput"
    outputDir = outDir+"/twitter_throughput_fig"
    trange = [100, 10000000]

    ploter = ExprPloter("Time (min)", ["#queries / #insertions"], semantics = set())
    ploter.addLines(LineDef({"":"line"}, {"app":"app", "query":"query"}, "time", "perf.query.count", 0))
    ploter.addLines(LineDef({"":"line"}, {"app":"app", "insert":"insert"}, "time", "perf.insert.count", 0))
    ploter.loadFiles(inputPath)
    config = {"bbox_to_anchor": (0.39, 1.2), "subplot.right":0.94,"subplot.top":0.78, "subplot.bottom": 0.13, "framealpha":1.0,
              "figsize":(9,9), 
              "columnspacing":1.2,"legsize":32,"round_x":True, "scalex":False, "scaley":True, "ylim":trange, "leg":('center', 2)}
    ploter.plotFigures(os.path.join(outputDir, "throughput"), config)

def plotUpdateScaleForWeibo():
    inputPath = dataDir+"/数据/weibo_result/update_scale"
    outputDir = outDir+"/weibo_result/update_scale_fig"

    ploter = ExprPloter("Percentage (%)", ["Compact Latency (min)", "Insert Latency (ms)"])
    ploter.addLines(LineDef({"":"line"}, {"app":"app", "insert total":"Compact"}, "size", "perf.insert_total.totalTime", 0))
    ploter.addLines(LineDef({"":"line"}, {"app":"app", "insert average":"Insert"}, "size", "perf.insert", 1))
    ploter.loadFiles(inputPath)
    config = {"bbox_to_anchor": (0.49, 1.35), "subplot.right":0.79,"subplot.top":0.78, "subplot.bottom": 0.14, "framealpha":1.0, "columnspacing":0.05,
              "figsize":(9,9),"legsize":32,
              "scalex":False, "scaley":False, "ylim":[10, 220], "ylim1":[0.05, 0.33], "leg":('upper center', 2)}
    ploter.plotFigures(os.path.join(outputDir, "scale"), config)

def plotUpdateScaleForTwitter():
    inputPath = dataDir+"/数据/twitter_result/update"
    outputDir = outDir+"/twitter_result/update_fig"

    # inputPath = dataDir+"dataset/scale/weibo"
    # outputDir = dataDir+"dataset/scale/weibo_fig"

    ploter = ExprPloter("Percentage (%)", ["Compact Latency (min)", "Insert Latency (ms)"])
    ploter.addLines(LineDef({"":"line"}, {"app":"app", "insert average":"Insert"}, "size", "perf.insert", 1))
    ploter.addLines(LineDef({"":"line"}, {"app":"app", "insert total":"Compact"}, "size", "perf.insert_total.totalTime", 0))
    ploter.loadFiles(inputPath)
    
    config = {"bbox_to_anchor": (-0.32, 1.35), "subplot.top":0.78, "subplot.bottom": 0.13,"subplot.right":0.85,
               "framealpha":1.0, "columnspacing":0.05,
              "scalex":False, "figsize":(9, 9),"scaley":False, 
              "ylim":[60, 150], "ylim1":[-0.25, 0.33], 
              "leg":('upper left', 2),"legsize":32}
    ploter.plotFigures(os.path.join(outputDir, "scale"), config)

def plotKeywordsForTwitter():
    inputPath = dataDir+"/数据/twitter_result/keywordnum"
    outputDir = outDir+"/数据/twitter_result/keywordnum_fig"
    #inputPath = dataDir+"/数据/keywordnum_50/twitter/part20"
    #outputDir = outDir+"/数据/keywordnum_50/twitter/part20_fig"
    yrange = [1, 200000]

    ploter = ExprPloter("|Q.K|", ["Latency (ms)"])
    ploter.addLines(LineDef({"offset":"offset", "width":"width", "k":"k", "size":"size"}, {"app":"app", "type":"type"}, "words", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    config = {"bbox_to_anchor": (0, 1), "subplot.top":0.98, "subplot.bottom": 0.13,"subplot.right":0.94,
              "leg":('upper left', 2), "legsize":32, "figsize":(9,9), "scalex":False, "scaley":True, "ylim":yrange, "suffix":"_words"}
    ploter.plotFigures(os.path.join(outputDir, "words"), config)

def plotKeywordsForWeibo():
    inputPath = dataDir+"/数据/weibo_result/keywordnum"
    outputDir = outDir+"/数据/weibo_result/keywordnum_fig"
    #inputPath = dataDir+"/数据/keywordnum_50/weibo/part20"
    #outputDir = outDir+"/数据/keywordnum_50/weibo/part20_fig"
    
    yrange = [1, 500000]

    ploter = ExprPloter("|Q.K|", ["Latency (ms)"])
    ploter.addLines(LineDef({"offset":"offset", "width":"width", "k":"k", "size":"size"}, {"app":"app", "type":"type"}, "words", "TOTAL_TIME", 0))
    ploter.loadFiles(inputPath)
    config = {"bbox_to_anchor": (0, 1), "subplot.top":0.98, "subplot.bottom": 0.13,"subplot.right":0.94,
              "leg":('upper left', 2), "legsize":32, "figsize":(9,9), "scalex":False, "scaley":True, "ylim":yrange, "suffix":"_words"}
    ploter.plotFigures(os.path.join(outputDir, "words"), config)

if __name__ == "__main__":
    # dataDir+"dataset/weiboexpr/2015_12_03/raw"
    #plotAllForWeibo()
    #plotLimitForWeibo()
    #plotScaleForWeibo()
    #plotKeywordsForWeibo()
    #plotUpdateScaleForWeibo()
    plotThroughputForWeibo()
    
    #plotLimitForTwitter()
    #plotThroughputForTwitter()
    #plotKeywordsForTwitter()
    #plotScaleForTwitter()
    #plotAllForTwitter()
    #plotUpdateScaleForTwitter()
