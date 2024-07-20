#!/usr/bin/python

def getPerfStm (check,val,warning,critical,Min=None,Max=None) :
        perf = ''
        if critical != '0' :
                perf = check + '=' + str(val) + ';' +  str(warning) + ';' + str(critical)   
        elif critical == '0' :
                perf = check + '=' + str(val) + ';' +  str(warning)   
        return perf

