from dataclasses import dataclass
import pyshark
import nest_asyncio
from pyshark.packet.packet import Packet
from tqdm import tqdm
import os
from datetime import date 
import pandas as pd 
import numpy as np 
from concurrent.futures import ThreadPoolExecutor
import concurrent
import time
import multiprocessing
import datetime

@dataclass
class PacketInfo:
    seq_number: int 
    masked_src: str 
    masked_dst: str 
    cap_index: int
    src_port: int 
    dst_port: int
    mallicious: bool # this requires a more work
    date: date
    seq_ip_version: str

# Apply the nest_asyncio patch


def convert_to_ip_str(ipnum):
    a = (ipnum & (255 << 0)) >> 0
    b = (ipnum & (255 << 8)) >> 8
    c = (ipnum & (255 << 16)) >>16
    d = (ipnum & (255 << 24)) >> 24
    return f"{d}.{c}.{b}.{a}"

def check_if_mallicious(dstport_number, srcport_number, seq_num):
    dstport_number = int(dstport_number)
    srcport_number = int(srcport_number)
    dst_contains = dstport_number in [23, 2323, 23231, 5555, 7547]
    src_contains = srcport_number in [23, 2323, 23231, 5555, 7547]
    return dst_contains or src_contains

def get_n_packages(file_name, number_of_packages):
    cap = pyshark.FileCapture(file_name)
    return_array = []

    s = file_name.split("/")[-1].replace("1400.pcap", "")
    current_date = date(year=int(s[:4]), month=int(s[4:6]), day=int(s[6:]))

    progress_bar = tqdm(total=number_of_packages, unit='iB', unit_scale=True)

    # Iterate over packets and display their information
    for i, packet in enumerate(cap):
        try:
            if i >= number_of_packages: break
            packet: Packet = packet
            if ('TCP' not in packet) or ('IP' not in packet): continue
            progress_bar.update(1)
            # print(f"Seq: {packet.tcp.seq}")
            a = PacketInfo(
                seq_number=int(packet.tcp.seq),
                masked_dst=packet.ip.dst,
                masked_src=packet.ip.src,
                cap_index=i,
                date=current_date,
                dst_port=int(packet.tcp.dstport),
                src_port=int(packet.tcp.srcport),
                mallicious=check_if_mallicious(packet.tcp.dstport, packet.tcp.srcport, packet.tcp.seq),
                seq_ip_version=convert_to_ip_str(int(packet.tcp.seq))
            )
            return_array.append(a)
        except Exception:
            continue

    cap.close()
    return return_array


def worker_function(filename, number_of_packages, queue):
    result = get_n_packages(filename, number_of_packages)
    # result = []
    queue.put(result)

    print("process done")
    


if __name__ == "__main__":
    # number_of_packages = 1_000_000
    number_of_packages = 1_000_000
    filenames = ["./extracted/" + x for x in os.listdir("./extracted/") if not x.startswith(".")]

    queue = multiprocessing.Queue()
    processes = []

    for ff in filenames:
        process = multiprocessing.Process(target=worker_function, args=(ff, number_of_packages, queue))
        process.start()
        processes.append(process)

    total_files = []

    for _ in filenames:
        total_files += queue.get() # optimmize this

    df = pd.DataFrame(total_files)

    df.to_csv(f"{datetime.datetime.now().date()}.csv")


    print("Done! exitting")

