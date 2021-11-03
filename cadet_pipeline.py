import datetime
import json
import os

import select_log
import utils


def get_node_uuid(node_logs_path):
    node_id_set = set()
    with open(node_logs_path, 'r', encoding='utf-8') as nf:
        for line in nf:
            node = json.loads(line)
            uuid = get_uuid4node(node)
            if uuid is not None:
                node_id_set.add(uuid)
    return node_id_set


def get_uuid4node(node):
    for k, v in node.items():
        if isinstance(v, dict):
            tmp = get_uuid4node(v)
            if tmp is not None:
                return tmp
        if k == "uuid":
            return v
    return None


def check_edge_by_node(edge, node_id_set):
    another_nodes = set()
    ok = False
    for k, v in edge.items():
        if isinstance(v, dict):
            if v.__contains__("subject"):
                if utils.check_special_values(v["subject"], node_id_set) or utils.check_special_values(
                        v["predicateObject"], node_id_set):
                    subject_nodes = utils.get_all_values(v["subject"])
                    predicate_nodes = utils.get_all_values(v["predicateObject"])
                    if subject_nodes is not None:
                        another_nodes = another_nodes.union(set(subject_nodes).difference(node_id_set))
                    if predicate_nodes is not None:
                        another_nodes = another_nodes.union(set(predicate_nodes).difference(node_id_set))
                    ok = True
                return another_nodes, ok
            another_nodes, ok = check_edge_by_node(v, node_id_set)
            if ok:
                return another_nodes, ok
    return None, False


def get_another_nodes(edge_logs_path, node_id_set):
    result = set()
    with open(edge_logs_path, 'r', encoding='utf-8') as ef:
        for line in ef:
            edge = json.loads(line)
            another_nodes, ok = check_edge_by_node(edge, node_id_set)
            if ok:
                result = another_nodes.union(result)
    return result


def check_edge(source, dist):
    if source is None or dist is None:
        return False
    _, ok = check_edge_by_node(source, {'uuid': dist})
    return ok


def check_node(source, dist):
    if source is None or dist is None:
        return False
    return utils.check_special_kvs(source, dist)


if __name__ == '__main__':
    remote_ip = {
        "remoteAddress": {"81.49.200.166", "78.205.235.65", "200.36.109.214", "139.123.0.113", "152.111.159.139",
                          "154.143.113.18", "61.167.39.128", "25.159.96.207", "76.56.184.25", "155.162.39.48",
                          " 198.115.236.119", "25.159.96.207", "76.56.184.25", "155.162.39.48", "198.115.236.119",
                          "53.158.101.118", "98.15.44.232", "192.113.144.28", "25.159.96.207", "76.56.184.25",
                          "155.162.39.48", "198.115.236.119", "53.158.101.118"
                          }}

    file_path = "data_set"
    file_paths = utils.clean_data_files(utils.items_dir(file_path))
    path = os.path.join('dist', datetime.datetime.now().strftime('%Y%m%d_%H_%M_%S') + '.json')
    # select_log.multi_select_special_logs(file_paths, remote_ip, utils.check_special_kvs, path)
    uuid_set = get_node_uuid('dist/20211026_17_33_00.json')
    # select_log.multi_select_special_logs(file_paths, uuid_set, check_edge, path)
    another_uuids = get_another_nodes('dist/20211026_19_47_10.json', uuid_set)

    # select_log.multi_select_special_logs(file_paths, another_uuids, utils.check_special_values, path)
    uuid_set = uuid_set.union(another_uuids)
    another_uuids = get_another_nodes('dist/20211027_09_48_39.json', uuid_set)
    # select_log.multi_select_special_logs(file_paths, another_uuids, utils.check_special_values, path)
    uuid_set = uuid_set.union(another_uuids)
    another_uuids = get_another_nodes('dist/20211027_10_04_06.json', uuid_set)
    print(len(another_uuids))
