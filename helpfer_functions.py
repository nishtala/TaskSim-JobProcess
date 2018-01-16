#!/usr/bin/env python

#16-1-2018
#rajiv.nishtala@bsc.es
#12.54pm

from subprocess import PIPE, Popen
from glob import glob
from os import path, makedirs, system
from time import sleep

def FILE_LEN(fname):
    p = Popen(['wc', '-l', fname], stdout=PIPE, stderr=PIPE)
    result, err = p.communicate()
    if p.returncode != 0:
        raise IOError(err)
    return int(result.strip().split()[0])

def FILE_EXISTS(file_name):
    return path.exists(file_name)

def CHECK_ERROR(PDIR):
    ERROR_FLAG = True
    FILE       = glob(PDIR + '*.err')[0]
    if ( FILE_LEN(FILE) != 0 ):
        return ERROR_FLAG
    else:
        ERROR_FLAG = False
        return ERROR_FLAG

def GET_OUTPUT(cmd):
    p = Popen(cmd, stdout=PIPE, stderr=PIPE)
    result, err = p.communicate()
    if p.returncode != 0:
        raise IOError(err)
    return result

def FLAT_LIST(paramlist):
    params = list()
    for x in range(len(paramlist)):
        to_keep = paramlist[x][:-1] + (paramlist[x][-1][0],) + (paramlist[x][-1][1],)
        params.append(to_keep)
    return params

def WAIT_FOR_JOB(WAITMODE):
    forever = True
    while forever:
        sleep(3)
        for JOB_ID in WAITMODE:
            CHECK_ID     = ['squeue', '-h', '-j', JOB_ID]
            OUTPUT_CHECK = GET_OUTPUT(CHECK_ID)
            OUTPUT_CHECK = OUTPUT_CHECK.strip()
            OUTPUT_CHECK = OUTPUT_CHECK.split()
            if not len(OUTPUT_CHECK): WAITMODE.remove(JOB_ID)
        if not WAITMODE:
            forever = False
    pass

def RECURSIVE_DIRECTORIES(path):
    try:
        makedirs(path)
    except OSError:
        pass

def CHANGE_CONFIG(TYPE, NEW_VALUE, TEMPLATE):
    CHANGE = "sed -i -re 's/(" + TYPE + ")[^=]*$/\\1'" + NEW_VALUE + "'/'" + " " + TEMPLATE
    system(CHANGE)
