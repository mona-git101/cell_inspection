import os
from typing import Dict , Union , Tuple

import pandas as pd
import re
import json
import requests
import linecache

from logHandler import LogHandler


def get_fileInfo_idv(directory):
    print(directory)
    split_dir = directory.split('/')
    print(split_dir)
    filename = split_dir[-1]
    ch = split_dir[-2]
    date = split_dir[-3]
    return filename , date , ch


def read_data(file):
    df_eis = pd.read_csv(file)

    data = {
        'zre': df_eis['R-complex'].values[:] ,
        '-zim': -df_eis['X-complex'].values[:] ,
        'voltage': df_eis['Voltage'].values[0]
    }
    return data


def read_data_bza(file):
    df_eis = pd.read_csv(file , skiprows=27)

    data = {
        'zre': df_eis['Zreal (Ω)'].values[:] ,
        '-zim': -df_eis['Zimag (Ω)'].values[:] ,
        'voltage': df_eis['Vdc (V)'].values[0]
    }
    return data


def read_data_bza_txt_long(file):
    df_eis = pd.read_csv(file , sep="\t" , skiprows=10 , encoding="cp949")
    f = open(file)
    lines = f.readlines()
    vol_line = lines[6]
    vol_str = vol_line[-7:-2]
    vol = float(vol_str)
    print(vol)
    data = {
        'zre': df_eis['Zre(ohm)'].values[[5 , 11 , 14]] ,
        '-zim': -df_eis['Zim(ohm)'].values[[5 , 11 , 14]] ,
        'voltage': vol
    }
    return data


def read_data_hioki(file):
    df_eis = pd.read_csv(file)

    data = {
        'zre': df_eis['R(ohm)'].values[:] ,
        '-zim': -df_eis['X(ohm)'].values[:] ,
        'voltage': df_eis['V(V)'].values[0]
    }
    return data


def sendData(processdata , processdataName) -> Tuple[bool , Dict]:
    is_ret: bool = False
    result: Dict = {}

    if processdata is None or processdataName is None:
        LogHandler.logger.warning(msg="processdata, processdataName is Null")
        return is_ret

    # 주소로 보내기
    try:
        res: requests.models.Response = requests.post(
            url='http://211.210.124.5:7777/api/mona/processData/' ,
            data={
                'processdata': json.dumps(processdata) ,
                'processdataName': processdataName
            })
        if res.ok:
            is_ret = True
            result = res.json()

    except Exception as ex:
        LogHandler.logger.error(msg=f"sendData ({processdataName} 를 전송하는중에 에러가 발생했습니다.\n{processdata}")

    return (is_ret , result)
