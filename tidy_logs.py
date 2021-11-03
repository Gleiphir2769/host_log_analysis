import datetime
import os
import sys

from tqdm import tqdm
import json
import multiprocessing as mp

import utils

data_type_set = set()

data_set = list()


def get_all_keys(dt):
    all_key = []
    for k in dt:
        if isinstance(dt[k], dict):
            all_key += get_all_keys(dt[k])
        all_key.append(k)
    return all_key


def convert(item):
    return hash(item)


def check_type(data_type):
    return data_type_set.__contains__(data_type)


def select_unique(file_name):
    with open(file_name, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            data_type = convert("".join(get_all_keys(data)))
            if check_type(data_type):
                continue
            data_type_set.add(data_type)
            data_set.append(data)

        dist_path = "/".join(file_name.split("/")[:-1]) + '/dist/'
        if not os.path.exists(dist_path):
            os.makedirs(dist_path)
        with open(dist_path + file_name.split("/")[-1] + '_dist_data.json', 'w', encoding='utf-8') as df:
            for dd in data_set:
                df.write(json.dumps(dd) + '\n')
        return len(data_set)


def multi_select_unique(data_set_path):
    start_t = datetime.datetime.now()
    num_cores = int(mp.cpu_count())
    print("本地计算机有: " + str(num_cores) + " 核心")
    file_paths = utils.clean_data_files(utils.items_dir(data_set_path))

    with mp.Pool(num_cores) as pool:
        res = tqdm(pool.imap(select_unique, file_paths), total=len(file_paths),
                   desc='使用' + str(num_cores) + '核心多进程分析json：')
        for r in res:
            print("筛选得到：{} 条数据\n".format(r))

    pool.close()
    pool.join()
    end_t = datetime.datetime.now()
    elapsed_sec = (end_t - start_t).total_seconds()
    print("多进程计算 共消耗: " + "{:.2f}".format(elapsed_sec) + " 秒")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    multi_select_unique('data_set/ta1-cadets-e3-official/ta1-cadets-e3-official.json')
