# encoding:utf8
import sys
import os
import json
from optparse import OptionParser


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