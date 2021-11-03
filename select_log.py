import datetime
import logging
import os
import queue
import sys
import threading
import multiprocessing as mp
from tqdm import tqdm
import json
from collections import deque

import utils

mutex = threading.Lock()




def select_special_logs(task_name, file_path, output_path, skey_set, check_func):
    buffer = queue.Queue(1024 * 1024)
    event = threading.Event()
    t = threading.Thread(target=write_special_logs, args=(output_path, event, buffer))
    t.start()
    with open(file_path, 'r', encoding='utf-8') as f:
        count = 0
        for line in tqdm(f):
            try:
                data = json.loads(line)

                if check_func(data, skey_set):
                    buffer.put(data, block=True, timeout=1)
                    count += 1

            except queue.Full:
                logging.warning("入队异常阻塞，检查出队线程")
        print(file_path + " 筛选得到：{} 条数据\n".format(count))

    event.set()
    t.join()

    return {task_name: True}


def write_special_logs(output_path, event, buffer):
    output_dir = "/".join(output_path.split("/")[:-1])
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
        except OSError:
            pass

    mutex.acquire()
    if not os.path.exists(output_path):
        file = open(output_path, 'w', encoding='utf-8')
        file.close()
    mutex.release()

    with open(output_path, 'a',
              encoding='utf-8') as df:
        while True:
            try:
                special_logs = buffer.get(block=True, timeout=1)
                df.write(json.dumps(special_logs) + '\n')
            except queue.Empty:
                if event.isSet():
                    break


def multi_select_special_logs(path_list, skey_set, check_func, output_path):
    start_t = datetime.datetime.now()
    num_cores = int(mp.cpu_count())
    print("本地计算机有: " + str(num_cores) + " 核心")

    task_names = ['task' + str(v) for v in range(len(path_list))]
    pool = mp.Pool(num_cores)
    param_dict = zip(task_names, path_list)
    results = [pool.apply_async(select_special_logs, args=(name, param, output_path, skey_set, check_func)) for
               name, param
               in
               param_dict]
    for p in results:
        print(p.get())

    end_t = datetime.datetime.now()
    elapsed_sec = (end_t - start_t).total_seconds()
    print("多进程计算 共消耗: " + "{:.2f}".format(elapsed_sec) + " 秒")


def serial_multi_select_special_logs(path_list, skeys_list, check_func, attack_list):
    if len(skeys_list) != len(attack_list):
        print("num of skeys '{}' are not matched num of attack_list '{}'".format(len(skeys_list), len(attack_list)))
        sys.exit(-1)
    for i in range(len(skeys_list)):
        multi_select_special_logs(path_list, skeys_list[i], check_func, os.path.join('dist', 'attack_list[i]',
                                                                                     datetime.datetime.now().strftime(
                                                                                         '%Y%m%d_%H_%M_%S') + '.json'))
