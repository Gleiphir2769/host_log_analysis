import utils
from select_log import serial_multi_select_special_logs, check_ip_port

if __name__ == '__main__':
    skeys_list = []
    attack_list = ["20180406-1100",
                   "20180411-1500",
                   "20180412-1400",
                   "20180413-CADETS",
                   "20180406-1500"]
    skeys_list.append(
        {"remoteAddress": {"81.49.200.166", "78.205.235.65", "200.36.109.214", "139.123.0.113", "152.111.159.139",
                           "154.143.113.18", "61.167.39.128"},
         "attack_path": {"81.49.200.166:80-128.55.12.167:8000", "78.205.235.65:80-128.55.12.167:8001",
                         "200.36.109.214:80-128.55.12.167:8002", "139.123.0.113:80-128.55.12.167:8003",
                         "152.111.159.139:80-128.55.12.167:8004", "154.143.113.18:80-128.55.12.167:8005",
                         "61.167.39.128:80-128.55.12.167:8006"}})
    skeys_list.append(
        {"remoteAddress": {"25.159.96.207", "76.56.184.25", "155.162.39.48", " 198.115.236.119"},
         "attack_path": {"25.159.96.207:80-128.55.12.167:8040", "76.56.184.25:80-128.55.12.167:8041",
                         "155.162.39.48:80-128.55.12.167:8042", "198.115.236.119:80-128.55.12.167:8043"}}
    )

    skeys_list.append(
        {"remoteAddress": {"25.159.96.207", "76.56.184.25", "155.162.39.48", "198.115.236.119", "53.158.101.118",
                           "98.15.44.232", "192.113.144.28"},
         "attack_path": {"25.159.96.207:80-128.55.12.167:8040", "76.56.184.25:80-128.55.12.167:8041",
                         "155.162.39.48:80-128.55.12.167:8042", "198.115.236.119:80-128.55.12.167:8043",
                         "53.158.101.118:80-128.55.12.167:8044", "98.15.44.232:80-128.55.12.167:8062",
                         "192.113.144.28:80-128.55.12.167:8063"}}
    )

    skeys_list.append(
        {"remoteAddress": {"25.159.96.207", "76.56.184.25", "155.162.39.48", "198.115.236.119", "53.158.101.118"},
         "attack_path": {"25.159.96.207:80-128.55.12.167:8040", "76.56.184.25:80-128.55.12.167:8041",
                         "155.162.39.48:80-128.55.12.167:8042", "198.115.236.119:80-128.55.12.167:8043",
                         "53.158.101.118:80-128.55.12.167:8044"}}
    )

    skeys_list.append(
        {"remoteAddress": {"62.83.155.175:80"},
         "attack_path": {"62.83.155.175:80-128.55.12.167:8007"}}
    )

    file_path = "data_set/ta1-cadets-e3-official.json"
    # skeys = {"remoteAddress": {"128.55.12.10"}, "remotePort": {53}}
    # file_path = "data_set/ta1-cadets-e3-official-1.json/dist/dist_ta1-cadets-e3-official-1.json"

    file_paths = utils.clean_data_files(utils.items_dir(file_path))

    serial_multi_select_special_logs(file_paths, skeys_list, check_ip_port, attack_list)