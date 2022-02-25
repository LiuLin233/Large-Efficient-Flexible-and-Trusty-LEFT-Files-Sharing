import argparse
import json
import os
import random
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
import socket

#######################【fileManager part】######################

log_path = r"log/file_record"
ip = None
transfer_rate = 225280
bind_port = 20000
my_file_dic = {}
list_state = 0
pool = None


class ACTION:
    SEND_FILE_LIST = "send_file_list"
    SEND_FILE = "send_file"
    REQUEST_FILE = "request_file"


def _argparse():
    parser = argparse.ArgumentParser(description="This is used to specify the ip address")
    parser.add_argument('--ip', action='store', required=True,
                        dest='ip', help='The ip address of client')
    return parser.parse_args()


#####################【server part】###########################
def run_server(port):
    with socket.socket() as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('', port))
        server.listen(50)
        while True:
            conn, addr = server.accept()
            service = Thread(target=server_handle_connection, args=(conn,))
            service.start()


def server_helper(port):
    helper = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    helper.bind(('', port))
    helper.listen(5)
    helper_conn, addr = helper.accept()
    return helper_conn


def recv_comp_file_lst(port, header_dic):
    conn = server_helper(port)
    body_len = header_dic['body_len']
    # body = recv_body(conn, body_len)
    body = conn.recv(body_len).decode('utf-8')
    peer_file_dic = json.loads(body)
    # 2、compare peer_file_dic & self_file_dic
    my_file_dic = get_my_file_dic()
    if not peer_file_dic: return
    for filepath, file_info in peer_file_dic.items():
        if my_file_dic.get(filepath):
            if my_file_dic.get(filepath).get('state') == "receiving":
                # receiving --> client.request_file(filename, pos)
                f = open(filepath, "ab")
                pos = f.tell()
                f.close()
                request_file(filepath, pos)
            elif my_file_dic[filepath]["remote_lm_time"] < file_info["remote_lm_time"]:
                # remote_change --> client.request_file(filename, 0)
                request_file(filepath)
        else:
            # remote news --> client.request_file(filename, 0)
            request_file(filepath)


def recv_file(port, header_dic):
    receiver = server_helper(port)
    file_info = header_dic['file_info']
    pos = header_dic['pos']
    body_len = header_dic['body_len']
    filepath = file_info['filepath']
    file_info['state'] = "receiving"
    update_my_file_dic({filepath: file_info})
    recv_size = 0

    print("[DOWNLOADING] %s" % filepath)

    file_dir = os.path.split(filepath)[0]
    if not os.path.isdir(file_dir):
        os.makedirs(file_dir)
    if pos == 0 and os.path.exists(filepath):
        f = open(filepath, 'r+')
        f.truncate()
        f.close()
    try:
        with open(filepath, 'ab') as f:
            while recv_size < body_len:
                recv_fragment = receiver.recv(min(body_len - recv_size, transfer_rate))
                if not recv_fragment:
                    print("break")
                    break
                recv_size += len(recv_fragment)
                f.write(recv_fragment)
        print("[FINISH DOWNLOADING] %s" % filepath)
        file_info['local_lm_time'] = int(os.stat(filepath).st_mtime)
        file_info['state'] = "done"
        update_my_file_dic({filepath: file_info})
    except Exception as e:
        print(e, 'eeeeeeeeee')
    finally:
        receiver.close()


def server_handle_connection(conn):
    global list_state
    try:
        header_len = struct.unpack('i', conn.recv(4))[0]
        recv_header = conn.recv(header_len).decode('utf-8')
        header_dic = json.loads(recv_header)
        action = header_dic["action"]
        # receive peer_file_dic [filename,size,lastModifiedTime]
        if action == ACTION.SEND_FILE_LIST:
            port = header_dic["port"]
            list_state += 1
            pool.submit(recv_comp_file_lst, port, header_dic)
            # recv_comp_file_lst(port, header_dic)
            Thread(target=send_my_file_dic, args=(True,)).start()
        # receive_file
        if action == ACTION.SEND_FILE:
            port = header_dic["port"]
            pool.submit(recv_file, port, header_dic)
        # handle_requestFile
        if action == ACTION.REQUEST_FILE:
            filepath = header_dic['filepath']
            pos = header_dic['pos']
            Thread(target=send_file, args=(filepath, pos)).start()
    except Exception as e:
        print('lose connect', e)
    finally:
        conn.close()


#########################【client part】##########################
def run_client(ip, port):
    helper_p = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connect_state = -1
    while connect_state != 0:
        try:
            connect_state = helper_p.connect_ex((ip, port))
        except Exception as e:
            print(e)
            connect_state = -1
    return helper_p


def make_send_header(header_dic):
    header = json.dumps(header_dic).encode('utf-8')
    header_len = struct.pack('i', len(header))
    p = run_client(ip, bind_port)
    p.send(header_len)
    p.send(header)


def request_file(filepath, pos=0):
    global random_port
    print("[request_file]" + filepath)
    header_dic = {
        "action": ACTION.REQUEST_FILE,
        "body_len": 0,
        "filepath": filepath,
        "pos": pos,
    }
    make_send_header(header_dic)


def send_my_file_dic(wait=False):
    global list_state
    if wait:
        time.sleep(0.2)
        if list_state == 0:
            return
    port = random.randint(21000, 30000)
    my_file_dic = json.dumps(get_my_file_dic()).encode('utf-8')
    header_dic = {
        "action": ACTION.SEND_FILE_LIST,
        "body_len": len(my_file_dic),
        "port": port
    }
    list_state -= 1
    make_send_header(header_dic)
    p = run_client(ip, port)
    p.send(my_file_dic)


def send_file_body(sender, filepath, pos):
    try:
        file = open(filepath, 'rb')
        file.seek(pos, 0)
        while True:
            file_fragemnt = file.read(transfer_rate)
            if not file_fragemnt:
                break
            sender.send(file_fragemnt)
    except Exception as e:
        print(e, "[send_file_body]")
    finally:
        # time.sleep(0.1)
        sender.close()


def send_file(filepath, pos=0):
    print("[SENDING]: " + filepath)
    port = random.randint(21000, 30000)
    file_info = get_my_file_dic()[filepath]
    header_dic = {
        "action": ACTION.SEND_FILE,
        "body_len": os.stat(filepath).st_size,
        "file_info": file_info,
        "pos": pos,
        "port": port
    }
    make_send_header(header_dic)
    send_file_body(run_client(ip, port), filepath, pos)


#######################【fileManager part】######################
def run_file_manager():
    if not os.path.isdir("log"):
        os.mkdir("log")
    elif os.path.exists(log_path):
        with open(log_path, "r") as rstream:
            dic = rstream.read()
        update_my_file_dic(json.loads(dic))
    traverse()
    reverse_traverse()
    send_my_file_dic()
    while True:
        traverse(send=True)
        time.sleep(0.2)


def traverse(src='share', send=False):
    if not os.path.isdir("share"): return
    for file in os.listdir(src):
        filepath = os.path.join(src, file)
        if os.path.isdir(filepath):
            traverse(filepath, send)
        else:
            if filepath in my_file_dic.keys() and my_file_dic.get(filepath).get('state') == 'done':
                file_info = my_file_dic[filepath]
                file_stat = os.stat(filepath)
                actual_lm_time = int(file_stat.st_mtime)

                file_info["file_size"] = file_stat.st_size
                if file_info["local_lm_time"] != actual_lm_time:
                    print("local_lm_time", file_info["local_lm_time"], "actual_lm_time", actual_lm_time)
                    file_info["local_lm_time"] = file_info['remote_lm_time'] = actual_lm_time
                    if send:
                        send_file(filepath)
            elif filepath not in my_file_dic.keys():
                lm_time = int(os.stat(filepath).st_mtime)
                file_info = {
                    "filepath": filepath,
                    "file_size": os.stat(filepath).st_size,
                    "local_lm_time": lm_time,
                    "remote_lm_time": lm_time,
                    "state": "done"
                }
                my_file_dic[filepath] = file_info
                if send:
                    send_file(filepath)
    filedic_log()


def reverse_traverse():
    for filepath in list(my_file_dic.keys()):
        if not os.path.exists(filepath):
            del my_file_dic[filepath]
    filedic_log()


def get_my_file_dic():
    return my_file_dic


def update_my_file_dic(item):
    my_file_dic.update(item)
    filedic_log()


def filedic_log():
    with open(log_path, 'w') as wstream:
        wstream.write(json.dumps(my_file_dic))


if __name__ == '__main__':
    parser = _argparse()
    ip = parser.ip

    pool = ThreadPoolExecutor()

    server_thread = Thread(target=run_server, name="run_server", args=(bind_port,))
    server_thread.start()

    file_manager = Thread(target=run_file_manager, name="run_file_manager")
    file_manager.start()

    file_manager.join()
    server_thread.join()
    pool.shutdown()
