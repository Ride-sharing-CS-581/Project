import os
from multiprocessing import Pool


processes = ('process1.py', 'process2.py', 'process3.py')


def run_process(process):
    os.system('python {}'.format(process))


pool = Pool(processes=3)
pool.map(run_process, processes)      