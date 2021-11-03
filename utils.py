import os
import shutil
from typing import List
from shutil import rmtree


def items_dir(root_path):
    l = []
    if os.path.isfile(root_path):
        return [root_path]
    for main_dir, dirs, file_name_list in os.walk(root_path):
        for file in file_name_list:
            file_path = os.path.join(main_dir, file)
            l.append(file_path)
    return l


def clean_data_files(files: List):
    for i in range(len(files)):
        tmp = files[i].split(".")
        if tmp[-1] != "json" and tmp[-2] != "json":
            #     files[i] = ".".join(tmp[:-1])
            # elif tmp[-1] != "json":
            files[i] = ""
    for inx, file in enumerate(files):
        if file == "":
            del files[inx]
    return files


def set_contains(source_set, dist_str):
    for item in source_set:
        if item.__contains__(dist_str):
            return True
    return False


def set_contained(source_set, dist_str):
    for item in source_set:
        if dist_str.__contains__(item):
            return True
    return False


def rm_empty(path):
    files = os.listdir(path)  # 获取路径下的子文件(夹)列表
    for file in files:
        print('Traversal at', file)
        if os.path.isdir(file):  # 如果是文件夹
            if not os.listdir(file):  # 如果子文件为空
                os.rmdir(file)  # 删除这个空文件夹
        elif os.path.isfile(file):  # 如果是文件
            if os.path.getsize(file) == 0:  # 文件大小为0
                os.remove(file)  # 删除这个文件
    print(path, 'Dispose over!')


def rm_spec_dir(root, name):
    cnt = 0
    for main_dir, dirs, file_name_list in os.walk(root):
        for dir in dirs:
            dir_path = os.path.join(main_dir, dir)
            if dir_path.split('/')[-1] == name:
                shutil.rmtree(dir_path)
                cnt += 1
    print(root, 'Dispose over!', cnt, "target directions have been removed")


def rm_spec_file(root, name):
    cnt = 0
    for main_dir, dirs, file_name_list in os.walk(root):
        for file in file_name_list:
            file_path = os.path.join(main_dir, file)
            if file.split('/')[-1] == name:
                os.remove(file_path)
                cnt += 1

    print(root, 'Dispose over!', cnt, "target files have been removed")


def get_all_keys(dt):
    if dt is None:
        return None
    all_key = []
    for k in dt:
        if isinstance(dt[k], dict):
            all_key += get_all_keys(dt[k])
        all_key.append(k)
    return all_key


def get_all_values(dt):
    if dt is None:
        return None
    all_value = []
    for _, v in dt.items():
        if isinstance(v, dict):
            all_value += get_all_values(v)
        all_value.append(v)
    return all_value


def check_special_keys(source, dist):
    if source is None or dist is None:
        return False
    data_keys = get_all_keys(source)
    for k in data_keys:
        if dist.__contains__(k):
            return True
    return False


def check_special_kvs(source, dist):
    if source is None or dist is None:
        return False
    for k, v in source.items():
        if dist.__contains__(k) and v in dist[k]:
            return True
        if isinstance(v, dict):
            if check_special_kvs(v, dist):
                return True
    return False


def check_special_values(source, dist):
    if source is None or dist is None:
        return False
    for _, v in source.items():
        if isinstance(v, dict):
            if check_special_values(v, dist):
                return True
        else:
            if v is not None and dist.__contains__(str(v)):
                return True
    return False


def check_ip_port(source, dist):
    if source is None or dist is None:
        return False
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
                        "string") and set_contained(dist["path"], v["predicateObjectPath"]["string"])) or (
                        v["predicateObject2Path"] is not None and v["predicateObject2Path"].__contains__(
                    "string") and set_contained(dist["path"], v["predicateObject2Path"]["string"])):
                    return True
                return False
            if check_with_path(v, dist):
                return True
    return False


def ip_port(ip, port):
    return str(ip) + ":" + str(port)


if __name__ == '__main__':
    rm_spec_dir('data_set', 'dist')
