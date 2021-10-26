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


if __name__ == '__main__':
    rm_spec_dir('data_set', 'dist')
