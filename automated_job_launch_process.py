#!/usr/bin/env python

#16-1-2018
#rajiv.nishtala@bsc.es
#12.54pm

import argparse
from os import system, chdir, listdir
from shutil import copy2
from time import sleep
from sys import argv
from multiprocessing import Pool
from itertools import product
import helper_functions as hf
from glob import glob

def BASE_PATHS(PDIR):
    TRACE          = glob(PDIR + '/TRACE*/')[0]
    A1_PRESIM      = TRACE + "/SIMULATION/A1_PRESIM/"
    A2_INTEGRATION = TRACE + "/SIMULATION/A2_INTEGRATION_PRESIM/"
    return A1_PRESIM, A2_INTEGRATION

def PRESIM(A1_PRESIM, TRACE_TYPE, RANK, MEMORY_MODE, D0, D1, BENCHMARK):
    MAP_FILE.write("... Processing paraver for %s with rank: %s ; memory mode : %s ; local : %s ;\
            remote : %s ; trace_type:  %s \n" % (BENCHMARK, RANK, MEMORY_MODE, D0, D1, TRACE_TYPE))
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

def INTEGRATION(A1_PRESIM, A2_INTEGRATION, MEMORY_MODE, TRACE_TYPE, RANK, D0, D1, BENCHMARK):
    MAP_FILE.write("... Processing dimemas for %s with rank: %s ; memory mode : %s ; local : %s ;\
            remote : %s ; trace_type:  %s \n" % (BENCHMARK, RANK, MEMORY_MODE, D0, D1, TRACE_TYPE))
    chdir(A2_INTEGRATION)
    DIMEMAS_SIM       = 'bash 02_Dimemas_sim.bash'
    system(DIMEMAS_SIM)
    MOD_AVAIL         = "sed -i '2imodule use -a /home/bsc15/bsc15755/nishtala_musa_module/musa2.7/module/' "
    MOD_LOAD          = "sed -i '3imodule load nishtala_musa' "
    for NUMCORES in CORES:
        FILE_DIM      = '03_DimemasTS_sim_presim_0000' + NUMCORES + '.bash'
        ADD_AVAIL     = MOD_AVAIL + FILE_DIM
        system(ADD_AVAIL)
        ADD_LOAD      = MOD_LOAD  + FILE_DIM
        system(ADD_LOAD)
        PRESIM_FILE   = ['sed', '-n', '8p', FILE_DIM]
        PRESIM_PATH   = hf.GET_OUTPUT(PRESIM_FILE)
        PRESIM_PATH   = A1_PRESIM + PRESIM_PATH.strip().split("=")[1][13:]
        if hf.FILE_EXISTS(PRESIM_PATH):
            DIMEMAS       = 'bash ' + FILE_DIM + ' > /dev/null 2>/dev/null'
            system(DIMEMAS)
            sleep(0.2)
            OPERATING_FILE= BENCHMARK + '-M:' + MEMORY_MODE + '-C:' + NUMCORES + '-T:' + \
                    TRACE_TYPE + '-R:' + RANK + "-D0:" + D0 + "-D1:" + D1
            PARAVER_FILE  = A2_INTEGRATION + '/trace_SIMULATED/' +'MUSA_hydro_0000' + NUMCORES +\
                    'cores_presim.prv'
            if hf.FILE_EXISTS(PARAVER_FILE):
                OUTPUT    = READ_FILE(PARAVER_FILE)
                OUTPUT_FILE.write("%s, %0.3f\n" % (OPERATING_FILE, OUTPUT))
            else:
                MAP_FILE.write("... PARAVER FILE NOT FOUND for %s with rank: %s ; memory mode : %s \
                        ; local : %s ; remote : %s ; trace_type: %s; NUMCORES: %s \n" % (\
                        BENCHMARK, RANK, MEMORY_MODE, D0, D1, TRACE_TYPE, NUMCORES))
        else:
            MAP_FILE.write("... PRESIM_PATH NOT FOUND for %s with rank: %s ; memory mode : %s ;\
                    local : %s ; remote : %s ; trace_type: %s; NUMCORES: %s \n" % (BENCHMARK, \
                    RANK, MEMORY_MODE, D0, D1, TRACE_TYPE, NUMCORES))


def COPY_STUFF(LAUNCH_DIR, JOB_TEMPLATE, TASKSIM_TEMPLATE, BENCHMARK, INSTRUMENTED_BENCHMARK, \
        JOB_LIST, BASEDIR):
    hf.RECURSIVE_DIRECTORIES(LAUNCH_DIR)
    copy2(JOB_TEMPLATE, LAUNCH_DIR)
    copy2(TASKSIM_TEMPLATE, LAUNCH_DIR)
    copy2(BASEDIR + BENCHMARK, LAUNCH_DIR)
    copy2(BASEDIR + INSTRUMENTED_BENCHMARK, LAUNCH_DIR)
    chdir(LAUNCH_DIR)
    LAUNCH_JOB = ['sbatch', JOB_TEMPLATE]
    JOB_ID     = hf.GET_OUTPUT(LAUNCH_JOB)
    JOB_ID     = JOB_ID.strip().split()[-1]
    JOB_LIST.append(JOB_ID)
    return JOB_LIST

def SPAWN_ALL(param):
    TRACE_TYPE  = param[0]
    RANK        = param[1]
    MEMORY_MODE = param[2]
    D0          = param[3]
    D1          = param[4]
    PDIR = GPFS_BASEDIR + BENCHMARK.upper() + '/LATENCY/' + BENCHMARK + '-' + RANK + \
            '-' + TRACE_TYPE + '-' + MEMORY_MODE + "-" + D0 + "-" + D1 + "/"
    ERROR_FLAG = hf.CHECK_ERROR(PDIR)
    if not ERROR_FLAG:
        A1_PRESIM, A2_INTEGRATION = BASE_PATHS(PDIR)
        PRESIM(A1_PRESIM, TRACE_TYPE, RANK, MEMORY_MODE, D0, D1, BENCHMARK)
        sleep(0.2)
        CORRECTION = A1_PRESIM + '/correction.dat'
        if hf.FILE_EXISTS(CORRECTION):
            INTEGRATION(A1_PRESIM, A2_INTEGRATION, MEMORY_MODE, TRACE_TYPE, RANK, D0, D1, BENCHMARK)
        else:
            MAP_FILE.write("... NOT processing dimemas for %s with rank: %s ; memory mode : %s ; \
                    local : %s ; remote : %s ; trace_type: %s as CORRECTION wasn't found \n" % (\
                    BENCHMARK, RANK, MEMORY_MODE, D0, D1, TRACE_TYPE))

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


def main(args):
    global GPFS_BASEDIR, BENCHMARK, RESULTS, MAP_FILE, OUTPUT_FILE
    BENCHMARK               = str(args.benchmark[0])
    INSTRUMENTED_BENCHMARK  = str(args.instrumented[0]
    JOB_LIST                = list()
    BASE_DIR                = '/home/bsc15/bsc15755/'
    GPFS_BASEDIR            = '/gpfs/scratch/bsc15/bsc15755/BENCHMARKS/'
    LOG_FILE                = BASE_DIR + '/' + BENCHMARK.upper() + '/' + str(args.log_file[0])
    OUTPUT_FILE             = BASE_DIR + '/' + BENCHMARK.upper() + '/' + str(args.output_file[0])
    BUF_SIZE                = 0
    MAP_FILE                = open(LOG_FILE, 'a', BUF_SIZE)
    OUTPUT_FILE             = open(OUTPUT_FILE, 'a', BUF_SIZE)
    OUTPUT_FILE.write("FILE, ExecutionTime(s)")
    TASKSIM_TEMPLATE        = BASE_DIR + '/' + BENCHMARK.upper() + '/' + 'tasksim_twodrams.conf'
    JOB_TEMPLATE            = BASE_DIR + '/' + BENCHMARK.upper() + '/' + 'job_tracer_debug_new.bash'
    BENCHMARK_DIR_GPFS      = BASE_DIR + '/' + BENCHMARK.upper() + "/"
    RESULTS                 = 'results'
    RANKS                   = [ '4' ]
    #RANKS                   = [ '2', '4', '8', '16', '32' ]
    TRACES                  = { 'SMALL': '-i /home/bsc15/bsc15755/HYDRO/data/input_small/input_small.nml'}
    #TRACES                  = { 'SMALL': '-i /home/bsc15/bsc15755/HYDRO/data/input_small/input_small.nml',
                                #'BIG': '-i /home/bsc15/bsc15755/HYDRO/data/input_big/input_big.nml'}
    TRACES_NAME             = ['SMALL']
    #TRACES_NAME             = ['SMALL', 'BIG']
    MEMORY_MODE             = [ '1']
    #MEMORY_MODE             = [ '0', '1']
    CORES                   = [ '01', '02', '04', '08', '16' ]
    DRAM                    = [['85', '102'], ['170','204']]
    #DRAM                    = [['85', '102'], ['106','128'], ['128','153'], ['149','179'], ['170','204']]

    for trace, path_dir in TRACES.items():
        for memory in MEMORY_MODE:
            for rank in RANKS:
                for local, remote in DRAM:
                    MAP_FILE.write("... Launched %s with rank: %s ; memory mode : %s ; local : %s ;\
                            remote : %s ; trace_type:  %s\n" % (BENCHMARK, rank, memory, local, remote, trace))
                    job_name    = BENCHMARK + '-' + rank + '-' + memory + '-' + trace + '-' + local + '-' + remote
                    output_log  = job_name + '-%j.out'
                    error_log   = job_name + '-%j.err'
                    CONFIG_VALS = [path_dir, memory, rank, local, remote, job_name, output_log, error_log, \
                            JOB_TEMPLATE, TASKSIM_TEMPLATE]
                    CHANGE_CONFIG_VALUES(*CONFIG_VALS)
                    LAUNCH_DIR = GPFS_BASEDIR + '/' + BENCHMARK.upper() + '/LATENCY/' + BENCHMARK + '-' + rank +\
                            '-' + trace + '-' + memory + '-' + local + '-' + remote
                    ARGS_COPY_STUFF = [LAUNCH_DIR, JOB_TEMPLATE, TASKSIM_TEMPLATE, BENCHMARK, INSTRUMENTED_BENCHMARK,\
                            JOB_LIST, BENCHMARK_DIR_GPFS]
                    JOB_LIST   = COPY_STUFF(*ARGS_COPY_STUFF)
                    chdir(BENCHMARK_DIR_GPFS)
                    sleep(0.2)

    MAP_FILE.write("... waiting for jobs to finish\n")
    hf.WAIT_FOR_JOB(JOB_LIST)

    MAP_FILE.write("... processing jobs\n")
    paramlist    = hf.FLAT_LIST(list(product(TRACES_NAME, RANKS, MEMORY_MODE, DRAM)))
    pool         = Pool()
    spawn_test   = pool.map(SPAWN_ALL, paramlist)

    MAP_FILE.close()
    OUTPUT_FILE.close()

if __name__ == '__main__':
    parser = argparse.ArgumentPraser(description='Automate tasksim procedure')
    parser.add_argument('--benchmark'   , nargs=1, help='benchmark binary')
    parser.add_argument('--instrumented', nargs=1, help='instrumented benchmark binary')
    parser.add_argument('--log_file'    , nargs=1, help='log file of events; saved in the benchmarks main folder')
    parser.add_argument('--output_file' , nargs=1, help='output file with file and execution time; \
            saved in the benchmarks main folder')
    args = parser.parse_args()
    main(args)
