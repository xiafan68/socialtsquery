# encoding:utf8
import os
import fnmatch

def mergeTwitterQuery():
    dir = "/Users/kc/快盘/dataset/twitter_expr/keywordsbycount"
    files = os.listdir(dir)
    files = sorted(files)
    output = open(os.path.join(os.path.dirname(dir), "queryseed.txt"), "w")
    for file in fnmatch.filter(files, "part*"):
        file = os.path.join(dir, file)
        if os.path.isdir(file):
            continue  
        input = open(file)
        for line in input.readlines():
            fields = line.split("\t")
            list = fields[0:2]
            list.extend([str(long(field) / 1800000) for field in fields[3:]])
            print "\t".join(list)
            output.write("\t".join(list))
            output.write("\n")
        input.close()
    output.close()
    
def swapKeywordsAndTime(file):
    dir = os.path.dirname(file)
    output = open(os.path.join(dir, os.path.basename(file).replace(".txt", "_swap.txt")),"w")
    input = open(file)
    for line in input.readlines():
        line = line.rstrip()
        fields = line.split("\t")
        list = fields[0:2]
        times = fields[2:]
        times.extend(fields[0:2])
        print "\t".join(times)
        output.write("\t".join(times))
        output.write("\n")
    input.close()
    output.close()
    
if __name__ == "__main__":
    #mergeTwitterQuery()
    file="/Users/kc/快盘/dataset/time_series/nqueryseed.txt"
    swapKeywordsAndTime(file)