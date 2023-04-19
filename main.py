from detectfile import RealtimeMonitoring
from utils import *
import pandas as pd 
import os
import csv
import threading


# 세팅값 불러오기 (from criterion_data)
setting_dir='data/criterion_data/reference'
setting_file_name='info.csv'
setting_file_path=f'{setting_dir}/{setting_file_name}'

root='data/raw_data'
root_dir=root_dir=f'{root}'
total_ch_num=2

## thread
threads=[]
for i in range(1,total_ch_num+1):
    setting_thread=RealtimeMonitoring(root_dir, str(i), total_ch_num)
    print('Thread: ', i )
    t=threading.Thread(target=setting_thread.run, args=())
    t.start()
    threads.append(t)

