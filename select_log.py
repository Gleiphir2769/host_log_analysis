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

def get_all_keys(dt):
    all_key = []
    for k in dt:
        if isinstance(dt[k], dict):
            all_key += get_all_keys(dt[k])
        all_key.append(k)
    return all_key


def check_special_keys(source, dist):
    data_keys = get_all_keys(source)
    for k in data_keys:
        if dist.__contains__(k):
            return True
    return False


def check_special_kvs(source, dist):
    for k, v in source.items():
        if dist.__contains__(k) and v in dist[k]:
            return True
        if isinstance(v, dict):
            if check_special_kvs(v, dist):
                return True
    return False


def check_special_values(source, dist):
    for k, v in source.items():
        if dist.__contains__(v):
            return True
        if isinstance(v, dict):
            if check_special_values(v, dist):
                return True
    return False


def check_ip_port(source, dist):
    for k, v in source.items():
        if isinstance(v, dict):
            if v.__contains__("remoteAddress") and v["remoteAddress"] in dist["remoteAddress"]:
                attack_path = ip_port(v["remoteAddress"], v["remotePort"]) + "-" + ip_port(v["localAddress"],
                                                                                           v["localPort"])
                if attack_path in dist["attack_path"]:
                    return True
                return False
            if check_ip_port(v, dist):
                return True
    return False


def check_with_path(source, dist):
    for k, v in source.items():
        if isinstance(v, dict):
            if v.__contains__("predicateObjectPath"):
                if (v["predicateObjectPath"] is not None and v["predicateObjectPath"].__contains__(
                        "string") and utils.set_contained(dist["path"], v["predicateObjectPath"]["string"])) or (
                        v["predicateObject2Path"] is not None and v["predicateObject2Path"].__contains__(
                    "string") and utils.set_contained(dist["path"], v["predicateObject2Path"]["string"])):
                    return True
                return False
            if check_with_path(v, dist):
                return True
    return False


# def check_ip_port(source, dist):
#     is_ip = False
#     for k, v in source.items():
#         if k == "remoteAddress" and v in dist["remoteAddress"]:
#             is_ip = True
#         if k == "remotePort" and v in dist["remotePort"] and is_ip:
#             return True
#         if isinstance(v, dict):
#             if check_ip_port(v, dist):
#                 return True
#     return False

def ip_port(ip, port):
    return str(ip) + ":" + str(port)


def select_special_logs(task_name, file_path, output_path, skey_set, check_func):
    buffer = queue.Queue(1024 * 1024)
    # todo: check
    # output_path = os.path.join("dist", prefix)
    # dist_filename = "dist_"  + str(file_path.split("/")[-1])
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
    results = [pool.apply_async(select_special_logs, args=(name, param, output_path, skey_set, check_func)) for name, param
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
        multi_select_special_logs(path_list, skeys_list[i], check_func, os.path.join('dist', 'attack_list[i]', datetime.datetime.now().strftime('%Y%m%d_%H_%M_%S')+'.json'))
