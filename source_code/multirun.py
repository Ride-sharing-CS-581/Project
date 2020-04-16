import os
from multiprocessing import Pool
from os import path

path1 = "\"G:/Advanced dbms/Project/source_code/Forming_pairs_delay_algo.py\""
path2 = "\"\"G:/Advanced dbms/Project/source_code/Forming_pairs_delay_algo.py\" \"2016-05-17 03:01:00\" \"2016-05-17 06:00:00\" 7300000 7400000 7300000 7400000 \""
processes = (path1
             )
def f1(args):
    a, b, c = args[0] , args[1] , args[2]
    return a+b+c

def run_process(process):
    print('python {}'.format(process))
    os.system('python {}'.format(process))



if __name__ == '__main__':
    pool = Pool(processes=2)
    pool.map(run_process, processes)
