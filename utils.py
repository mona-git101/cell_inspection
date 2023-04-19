import os
import pandas as pd
import re
import json
import requests
import linecache

def get_fileInfo_idv(directory): 
    print(directory)
    split_dir=directory.split('/')
    print(split_dir)
    filename=split_dir[-1]
    ch=split_dir[-2]
    date=split_dir[-3]
    return filename, date, ch

def read_data(file):
    df_eis = pd.read_csv(file)

    data = {
        'zre': df_eis['R-complex'].values[:],
        '-zim': -df_eis['X-complex'].values[:],
        'voltage':df_eis['Voltage'].values[0] 
    }
    return data

def read_data_bza(file):
    df_eis = pd.read_csv(file, skiprows=27)

    data = {
        'zre': df_eis['Zreal (Ω)'].values[:],
        '-zim': -df_eis['Zimag (Ω)'].values[:],
        'voltage':df_eis['Vdc (V)'].values[0] 
    }
    return data

def read_data_bza_txt_long(file):
    df_eis = pd.read_csv(file, sep="\t",skiprows=10, encoding="cp949")
    f = open(file)
    lines=f.readlines()
    vol_line=lines[6]
    vol_str=vol_line[-7:-2]
    vol=float(vol_str)
    print(vol)
    data = {
        'zre': df_eis['Zre(ohm)'].values[[5,11,14]],
        '-zim': -df_eis['Zim(ohm)'].values[[5,11,14]],
        'voltage':vol 
    }
    return data

def read_data_hioki(file):
    df_eis = pd.read_csv(file)

    data = {
        'zre': df_eis['R(ohm)'].values[:],
        '-zim': -df_eis['X(ohm)'].values[:],
        'voltage':df_eis['V(V)'].values[0] 
    }
    return data

def sendData(processdata, processdataName):
    
    ## add null check processdata
    if processdata is None:
        print("processdata is NULL")
        return False  

    ## add null check processdataName    
    if processdataName is None:
        print("processdataName is NULL")
        return False

    dlist = processdata
    jsonObj = json.dumps(dlist)
    
    print(dlist)
    # 주소로 보내기
    res = requests.post('http://211.210.124.5:7777/api/mona/processData/', {
        'processdata' : jsonObj,
        'processdataName' : processdataName
                    })

    return res.json()



