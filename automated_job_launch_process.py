#!/usr/bin/env python

#16-1-2018
#rajiv.nishtala@bsc.es
#12.54pm

import argparse
from os import system, chdir, listdir
from shutil import copy2, move, rmtree, copy
from time import sleep
from sys import argv
from multiprocessing import Pool
from itertools import product, izip, repeat
import helper_functions as hf
from glob import glob

def BASE_PATHS(PDIR):
    TRACE          = glob(PDIR + '/TRACE*/')[0]
    SIMULATION     = TRACE + "/SIMULATION/"
    return TRACE, SIMULATION

def PRESIM(A1_PRESIM):
    JUNK       = set(['Submitted', 'batch', 'job'])
    JOB_PRESIM = ['sbatch', 'generate_musa_presim.bash']
    SUBMIT_JOB = ['bash', 'submit_everything.bash']
    EX_POLATE  = 'bash extrapolate_burst_duration.bash > /dev/null 2>/dev/null'
    NEW_TIME   = '06:00:00'
    chdir(A1_PRESIM)
    JOB_ID    = hf.GET_OUTPUT(JOB_PRESIM)
    JOB_ID    = JOB_ID.strip().split()[-1]
    sleep(2)
    hf.WAIT_FOR_JOB([JOB_ID])
    MPIRANKS   = glob('launch_musa*.bash')
    for MPIRANK in MPIRANKS: hf.CHANGE_CONFIG("time=", NEW_TIME, MPIRANK)
    OUTPUT     = hf.GET_OUTPUT(SUBMIT_JOB)
    sleep(2)
    OUTPUT     = OUTPUT.strip().split()
    WAITLIST   = list(set(OUTPUT) - JUNK)
    hf.WAIT_FOR_JOB(WAITLIST)
    sleep(0.2)
    system(EX_POLATE)
    sleep(0.2)

def READ_FILE(file_name):
    infile = open(file_name,'r')
    output = infile.readline()
    output = output.strip().split(":")
    #ns to s
    match  = float([s for s in output if '_ns' in s][0].split("_ns")[0])/1e9
    return match

def INTEGRATION(A1_PRESIM, A2_INTEGRATION, OPERATING_FILE, CORES):
    chdir(A2_INTEGRATION)
    DIMEMAS_SIM       = 'bash 02_Dimemas_sim.bash'
    system(DIMEMAS_SIM)
    for NUMCORES in CORES:
        FILE_DIM      = '03_DimemasTS_sim_presim_0000' + NUMCORES + '.bash'
        PRESIM_FILE   = ['sed', '-n', '8p', FILE_DIM]
        PRESIM_PATH   = hf.GET_OUTPUT(PRESIM_FILE)
        PRESIM_PATH   = A1_PRESIM + PRESIM_PATH.strip().split("=")[1][13:]
        if hf.FILE_EXISTS(PRESIM_PATH):
            DIMEMAS       = 'bash ' + FILE_DIM + ' > /dev/null 2>/dev/null'
            system(DIMEMAS)
            sleep(0.2)
            OPERATING_FILE= OPERATING_FILE + '-C:' + NUMCORES
            PARAVER_FILE  = A2_INTEGRATION + '/trace_SIMULATED/' +'MUSA_hydro_0000' + NUMCORES +\
                    'cores_presim.prv'
            if hf.FILE_EXISTS(PARAVER_FILE):
                OUTPUT    = READ_FILE(PARAVER_FILE)
                OUTPUT_FILE.write("%s, %0.3f\n" % (OPERATING_FILE, OUTPUT))
            else:
                MAP_FILE.write("... PARAVER file NOT found for %s\n" %(OPERATING_FILE))
        else:
            MAP_FILE.write("... PRESIM_PATH NOT FOUND in %s for executing paraver %s\n" % (A2_INTEGRATION , FILE_DIM))

def COPY_STUFF(LAUNCH_DIR, JOB_TEMPLATE, TASKSIM_TEMPLATE, BENCHMARK, INSTRUMENTED_BENCHMARK, \
        JOB_LIST, BASEDIR, DATASET):
    hf.RECURSIVE_DIRECTORIES(LAUNCH_DIR)
    copy2(JOB_TEMPLATE, LAUNCH_DIR)
    copy2(TASKSIM_TEMPLATE, LAUNCH_DIR)
    copy2(BASEDIR + BENCHMARK.lower(), LAUNCH_DIR)
    copy2(BASEDIR + INSTRUMENTED_BENCHMARK.lower(), LAUNCH_DIR)
    COPYDIR = 'cp -r ' + DATASET + ' ' + LAUNCH_DIR
    system(COPYDIR)
    chdir(LAUNCH_DIR)
    LAUNCH_JOB = ['sbatch', JOB_TEMPLATE]
    JOB_ID     = hf.GET_OUTPUT(LAUNCH_JOB)
    JOB_ID     = JOB_ID.strip().split()[-1]
    JOB_LIST.append(JOB_ID)
    return JOB_LIST

def CHANGE_CONFIG_VALUES(path_dir, memory, rank, local, \
        remote, job_name, output_log, error_log, \
        JOB_TEMPLATE, TASKSIM_TEMPLATE):
    #hf.CHANGE_CONFIG("ARGS=", path_dir, JOB_TEMPLATE)
    hf.CHANGE_CONFIG("MODE=", memory, JOB_TEMPLATE)
    hf.CHANGE_CONFIG("ntasks=", rank, JOB_TEMPLATE)
    hf.CHANGE_CONFIG("PR=", rank, JOB_TEMPLATE)
    hf.CHANGE_CONFIG("latency0 = ", local, TASKSIM_TEMPLATE)
    hf.CHANGE_CONFIG("latency1 = ", remote, TASKSIM_TEMPLATE)
    hf.CHANGE_CONFIG("job-name=", job_name, JOB_TEMPLATE)
    hf.CHANGE_CONFIG("output=", output_log, JOB_TEMPLATE)
    hf.CHANGE_CONFIG("error=", error_log, JOB_TEMPLATE)
    pass

def PROCESS_TRACES(params):
    SIM_DIR = params[0]
    BENCHMARK = params[1]
    MAP_FILE.write("... processing trace for %s in %s\n" % (BENCHMARK, SIM_DIR))
    A1_PRESIM = SIM_DIR + "/A1_PRESIM/"
    A2_INTEGRATION = SIM_DIR + "/A2_INTEGRATION_PRESIM/"
    SIM_SPLIT = SIM_DIR.split("/")
    RELEVANT  = filter(lambda element: BENCHMARK in element, SIM_SPLIT)
    BENCHMARK = RELEVANT[0]
    RANK      = RELEVANT[1].split("-")[1]
    TRACE_TYPE = RELEVANT[1].split("-")[2]
    MEMORY_MODE = RELEVANT[1].split("-")[3]
    LATENCY = filter(lambda element: "SIMULATION" in element, SIM_SPLIT)[0].split("-")
    D0 = LATENCY[1]
    D1 = LATENCY[2]
    OPERATING_FILE= BENCHMARK + '-M:' + MEMORY_MODE + '-T:' + \
                TRACE_TYPE + '-R:' + RANK + "-D0:" + D0 + "-D1:" + D1
    ALL_CONFIGS   = glob(A1_PRESIM + "*.conf")
    for CONFIG_FILE in ALL_CONFIGS:
        hf.CHANGE_CONFIG("latency0 = ", D0, CONFIG_FILE)
        hf.CHANGE_CONFIG("latency1 = ", D1, CONFIG_FILE)
    PRESIM(A1_PRESIM)
    sleep(0.2)
    CORRECTION = A1_PRESIM + '/correction.dat'
    if hf.FILE_EXISTS(CORRECTION) and MEMORY_MODE == '1':
        INTEGRATION(A1_PRESIM, A2_INTEGRATION, OPERATING_FILE, CORES)

def SIM_PARAMETERS(JOB_NAMES, DRAM):
    SIM_DIRS                = list()
    for job in JOB_NAMES:
        PDIR = GPFS_BASEDIR + BENCHMARK + "/" + job_name + "/"
        ERROR_FLAG = hf.CHECK_ERROR(PDIR)
        TRACE_DIR, SIMULATION_DIR = BASE_PATHS(PDIR)
        for local, remote in DRAM:
            chdir(TRACE_DIR)
            SIMULATION_FOLDER = TRACE_DIR + "SIMULATION-" + local + "-" + remote + "/"
            hf.copytree(SIMULATION_DIR, SIMULATION_FOLDER)
            SIM_DIRS.append(SIMULATION_FOLDER)
        rmtree(SIMULATION_DIR)
    return SIM_DIRS

def main(args):
    BUF_SIZE                = 0
    global GPFS_BASEDIR, BENCHMARK, MAP_FILE, OUTPUT_FILE
    BENCHMARK               = str(args.benchmark[0]).upper()
    INSTRUMENTED_BENCHMARK  = str(args.instrumented[0]).upper()
    DATASET                 = str(args.dataset[0])
    LOG_FILE                = str(args.log_file[0])
    MAP_FILE                = open(LOG_FILE, 'a', BUF_SIZE)
    OUTPUT_LOG              = str(args.output_file[0])
    OUTPUT_FILE             = open(OUTPUT_LOG, 'a', BUF_SIZE)
    JOB_NAMES               = list()
    JOB_LIST                = list()

    BASE_DIR                = '/gpfs/scratch/bsc15/bsc15755/BIN_BENCHMARKS/'
    GPFS_BASEDIR            = '/gpfs/scratch/bsc15/bsc15755/BENCHMARKS/'

    TASKSIM_TEMPLATE        = BASE_DIR + '/' + BENCHMARK + '/' + 'tasksim_twodrams.conf'
    JOB_TEMPLATE            = BASE_DIR + '/' + BENCHMARK + '/' + 'job_tracer.bash'
    BENCHMARK_DIR_GPFS      = BASE_DIR + '/' + BENCHMARK + "/"

    RANKS                   = [ '4' ]
    TRACES                  = { 'SMALL': '-i /home/bsc15/bsc15755/HYDRO/data/input_small/input_small.nml'}
    TRACES_NAME             = ['SMALL']
    MEMORY_MODE             = [ '1' ]
    CORES                   = [ '01', '02', '04', '08', '16' ]
    BASELINE                = [['85', '102']]
    DRAM                    = [['170','204']]
    #DRAM                    = [['85', '102'], ['170','204'], ['340','408'], ['680','816'], ['1360','1632']]

    OUTPUT_FILE.write("FILE, ExecutionTime(s)\n")

    for trace, path_dir in TRACES.items():
        for memory in MEMORY_MODE:
            for rank in RANKS:
                for local, remote in BASELINE:
                    MAP_FILE.write("... Launched %s with rank: %s ; memory mode : %s ; local : %s ;\
                            remote : %s ; trace_type:  %s\n" % (BENCHMARK, rank, memory, local, remote, trace))
                    job_name    = BENCHMARK + '-' + rank + '-' + trace + '-' + memory + '-' + local + '-' + remote
                    JOB_NAMES.append(job_name)
                    output_log  = "log-" + job_name + '-%j.out'
                    error_log   = "log-" + job_name + '-%j.err'
                    CONFIG_VALS = [path_dir, memory, rank, local, remote, job_name, output_log, error_log, \
                            JOB_TEMPLATE, TASKSIM_TEMPLATE]
                    CHANGE_CONFIG_VALUES(*CONFIG_VALS)
                    LAUNCH_DIR = GPFS_BASEDIR + '/' + BENCHMARK + '/' + BENCHMARK + '-' + rank +\
                            '-' + trace + '-' + memory + '-' + local + '-' + remote
                    ARGS_COPY_STUFF = [LAUNCH_DIR, JOB_TEMPLATE, TASKSIM_TEMPLATE, BENCHMARK, INSTRUMENTED_BENCHMARK,\
                            JOB_LIST, BENCHMARK_DIR_GPFS, DATASET]
                    JOB_LIST   = COPY_STUFF(*ARGS_COPY_STUFF)
                    chdir(BENCHMARK_DIR_GPFS)
                    sleep(0.2)

    MAP_FILE.write("... waiting for jobs to finish\n")
    hf.WAIT_FOR_JOB(JOB_LIST)

    MAP_FILE.write("... adding different simulation directories\n")
    SIM_DIRS = SIM_PARAMETERS(JOB_NAMES, DRAM)

    params    = izip(SIM_DIRS, repeat(BENCHMARK))
    pool      = Pool()
    spawn_test =pool.map(PROCESS_TRACES, params)

    MAP_FILE.close()
    OUTPUT_FILE.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Automate tasksim procedure. Benchmark in bin always in lower case. directories in uppercase. IT'LL FAIL OTHERWISE!!!")
    parser.add_argument('-b', '--benchmark'   , nargs=1, help='benchmark binary. always uppercase')
    parser.add_argument('-i', '--instrumented', nargs=1, help='instrumented benchmark binary. always uppercase')
    parser.add_argument('-d', '--dataset'     , nargs=1, help='dataset basepath')
    parser.add_argument('-l', '--log_file'    , nargs=1, help='log file of events; saved in the benchmarks main folder')
    parser.add_argument('-o', '--output_file' , nargs=1, help='output file with file and execution time; \
            saved in the benchmarks main folder')
    args = parser.parse_args()
    main(args)
