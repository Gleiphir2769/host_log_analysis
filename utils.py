import os
from typing import List


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


if __name__ == '__main__':
    print(clean_data_files(items_dir("data_set")))
