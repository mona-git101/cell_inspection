import datetime
from multiprocessing import RLock
from typing import Tuple , Dict

import numpy as np
import pandas
import pandas as pd
from sqlalchemy import create_engine

from logHandler import LogHandler


class dbconnect:
    # 쓰레딩을 사용할 경우, DB 커서 객체를 동시 트랜잭션이 발생하게 되면, Query 시 Packet Sequence Error가 발생하기 떄문에
    # Query 시 세마포어를 설정하여 DB 트랜잭션의 순서를 맞춰줍니다.
    rLock: RLock = RLock()

    def __init__(self):
        # self.engine = create_engine("mysql+pymysql://root:" + "mona1101!!" + "@127.0.0.1:3306/cei_dev1?charset=utf8" , encoding='utf-8')
        self.engine = create_engine("mysql+pymysql://root:" + "1234" + "@127.0.0.1:3306/cei_dev1?charset=utf8" , encoding='utf-8')  # TODO 변경
        self.conn = self.engine.connect()
        self.conn_raw = self.engine.raw_connection()

        # 커서 객체를 사용시에만 호출하기 때문에 주석처리

        # self.cur = self.conn_raw.cursor()

    def onewayLock(self , cursor , sql_command: str):
        """
        세마포어를 활용하여 race condtion을 방지할 수 있다.
        :param sql_command: SQL Query
        :return:
        """
        self.rLock.acquire(block=True , timeout=10)
        cursor.execute(sql_command)
        self.rLock.release()

    def getCursor(self):
        """
        DB 연결 객체를 사용시에만 cursor를 호출하는게 DB 트랜잭션 및 연결 관리에 깔끔합니다.
        Conneciton
        :return: DB 커서 연결 객체
        """
        cursor = self.conn_raw.cursor()
        return cursor

    def update_db(self , df , name_db):
        try:
            df.to_sql(name_db , con=self.engine , if_exists='append' , index=False)
        except Exception as ex:
            LogHandler.logger.error(msg=f"update_db error {name_db}\n{df}")

    def truncate_db(self , name_db: str):
        sql_command = f'truncate table {name_db}'
        cursor = self.getCursor()
        cursor.execute(sql_command)

    def upload_calmodel(self , ch_num , no , datetime_input , calmodel):
        LogHandler.logger.info(msg='uploading calmodel')
        # no의 경우 이전 모델을 불러올 때 db에서 정보 호출
        # name_db = calmodel, diamodel
        # no: model number = 1,2,3,..
        # date: 모델 생성 날짜 -> datetime으로 변경 필요
        raw_data = {
            'no': [no] ,
            'datetime': [datetime_input] ,
            'zre1': calmodel['zre'][0] ,
            'zre2': calmodel['zre'][1] ,
            'zre3': calmodel['zre'][2] ,
            'zim1': calmodel['-zim'][0] ,
            'zim2': calmodel['-zim'][1] ,
            'zim3': calmodel['-zim'][2]
        }

        df = pd.DataFrame(raw_data)
        self.update_db(df=df , name_db=f'calmodel{ch_num}')

    def upload_eis(self , ch_num: str , datetime_input: datetime.datetime , idx: int , measure: Dict):
        # date 형식: string
        LogHandler.logger.info(msg='uploading eis')

        if idx < 10:
            str_idx = f'000{idx}'
        elif idx < 100:
            str_idx = f'00{idx}'
        elif idx < 1000:
            str_idx = f'0{idx}'
        elif idx < 10000:
            str_idx = f'{idx}'
        else:
            LogHandler.logger.error(msg=f'idx should be lower than 10000')

        key = str(datetime_input.year)

        if datetime_input.month < 10:
            key = f'{datetime_input.year}0{datetime_input.month}'
        else:
            key = f'{key}{datetime_input.month}'

        if datetime_input.day < 10:
            key = f'{key}0{datetime_input.day}'

        else:
            key = f'{key}{datetime_input.day}'

        raw_data = {
            'key': [f'{key}{ch_num}{str_idx}'] ,
            'datetime': [datetime_input] ,
            'idx': [idx] ,
            'zre1': measure['zre'][0] ,
            'zre2': measure['zre'][1] ,
            'zre3': measure['zre'][2] ,
            'zim1': measure['-zim'][0] ,
            'zim2': measure['-zim'][1] ,
            'zim3': measure['-zim'][2]
        }
        df = pd.DataFrame(raw_data)
        self.update_db(df=df , name_db=f'raweis{ch_num}')

    def upload_feature(self , ch_num , datetime_input , idx , voltage , vol_max , vol_min , calibrated , diag_result):
        # date 형식: string
        LogHandler.logger.info(msg='uploading feature')

        if idx < 10:
            str_idx = '000' + str(idx)
        elif idx < 100:
            str_idx = '00' + str(idx)
        elif idx < 1000:
            str_idx = '0' + str(idx)
        elif idx < 10000:
            str_idx = str(idx)
        else:
            print('error, idx should be lower than 10000')

        key = str(datetime_input.year)
        if datetime_input.month < 10:
            key = key + '0' + str(datetime_input.month)
        else:
            key = key + str(datetime_input.month)
        if datetime_input.day < 10:
            key = key + '0' + str(datetime_input.day)
        else:
            key = key + str(datetime_input.day)
        key = key + str(ch_num) + str_idx

        if voltage > vol_max:
            vol_alarm = 1
        elif voltage < vol_min:
            vol_alarm = 1
        else:
            vol_alarm = 0
        status = vol_alarm + diag_result['diagnosis_alarm'] - diag_result['diagnosis_alarm'] * vol_alarm

        raw_data: Dict = {
            'key': [key] ,
            'datetime': [datetime_input] ,
            'idx': [idx] ,
            'voltage': voltage ,
            'zre1cal': calibrated['zre'][0] ,
            'zre2cal': calibrated['zre'][1] ,
            'zre3cal': calibrated['zre'][2] ,
            'zim1cal': calibrated['-zim'][0] ,
            'zim2cal': calibrated['-zim'][1] ,
            'zim3cal': calibrated['-zim'][2] ,
            'stadist1': diag_result['statistical_distances'][0] ,
            'stadist2': diag_result['statistical_distances'][1] ,
            'stadist3': diag_result['statistical_distances'][2] ,
            'status': status ,
            'diaalarm': diag_result['diagnosis_alarm'] ,
            'diaalarmidx1': diag_result['diagnosis_alarm_index'][0] ,
            'diaalarmidx2': diag_result['diagnosis_alarm_index'][1] ,
            'diaalarmidx3': diag_result['diagnosis_alarm_index'][2] ,
            'volalarm': vol_alarm ,
            'calalarm': diag_result['calibration_alarm'] ,
            'ohmalarm': diag_result['ohmic_alarm']
        }
        df = pd.DataFrame(raw_data)
        self.update_db(df=df , name_db=f'feature{ch_num}')
        return key

    def select_model(self , name_db , ch_num_str , no):
        tbl_name = f"{name_db}{ch_num_str}"
        if no == 'latest':
            sql_command = f"SELECT * FROM {tbl_name} ORDER BY no DESC LIMIT 1"  ## 수정 필요 (동작안함)
        else:
            sql_command = f"SELECT * FROM {tbl_name} WHERE no LIKE {no}"

        cursor = self.getCursor()
        self.onewayLock(cursor=cursor , sql_command=sql_command)
        row = cursor.fetchone()
        return row  # tuple 형태로 출력?

    def select_arb_data(self , name_db , ch_num_str , data_num , idx_name):
        tbl_name = f"{name_db}{ch_num_str}"

        if data_num == 'all':
            sql_command = f'SELECT * FROM {tbl_name} limit 0,4'

        elif data_num == 'latest':
            sql_command = f'SELECT * FROM {tbl_name} ORDER BY {idx_name} DESC LIMIT 1'

        else:
            sql_command = f"SELECT * FROM {tbl_name} ORDER BY {idx_name} DESC LIMIT {data_num}"

        cursor = self.getCursor()
        self.onewayLock(cursor=cursor , sql_command=sql_command)
        result: Tuple[int , str , datetime.datetime , float , float , float , float , float , float , float , float , float , float] = cursor.fetchall()
        return result

    def select_latest_data(self , name_db , ch_num , data_num , offset):
        tbl_name = f"{name_db}{ch_num}"

        if data_num == 'all':
            sql_command = f"SELECT * FROM {tbl_name} ORDER BY idx"
        else:
            # sql_command = "SELECT * FROM " + tbl_name + " ORDER BY idx LIMIT " + str(data_num)
            sql_command = f"SELECT * FROM {tbl_name} ORDER BY idx LIMIT {data_num}"

        cursor = self.getCursor()
        self.onewayLock(cursor=cursor , sql_command=sql_command)
        rows = cursor.fetchall()
        measures_db = []

        for row in rows:  #### 수정u
            zre = np.array([row[offset] , row[offset + 1] , row[offset + 2]])
            zim = np.array([row[offset + 3] , row[offset + 4] , row[offset + 5]])
            eis_data = {'zre': zre , '-zim': -zim}
            measures_db.append(eis_data)

        return measures_db

    def latest_idx(self , ch_num , pos_idx):
        """
        각 raweis[CH_NUM] 테이블의 마지막 idx 값을 가져온다
        :param ch_num:
        :param pos_idx:
        :return:
        """
        tbl_name: str = f'raweis{ch_num}'
        sql_command = f"SELECT * FROM {tbl_name} ORDER BY idx DESC LIMIT 1"
        cursor = self.getCursor()
        self.onewayLock(cursor=cursor , sql_command=sql_command)
        rows = cursor.fetchall()
        row = rows[0]
        idx = row[pos_idx]
        return idx

    def upload_tempjson(self , db_name , ch_num , key , datetime_input , measure , calibrated , diag_result):

        data: Dict = {
            'Ch': ch_num ,
            'Name': key ,
            'Time': datetime_input ,
            'Voltage': measure['voltage'] ,
            'Ohmicx': calibrated['zre'][0] ,
            'Ohmicy': calibrated['-zim'][0] ,
            "Electrode1x": calibrated['zre'][1] ,
            "Electrode1y": calibrated['-zim'][1] ,
            "Electrode2x": calibrated['zre'][2] ,
            "Electrode2y": calibrated['-zim'][2] ,
            "Ohmic_CP": diag_result['statistical_distances'][0] ,
            "Electrode1_CP": diag_result['statistical_distances'][1] ,
            "Electrode2_CP": diag_result['statistical_distances'][2]
        }

        df: pandas = pd.DataFrame(data , index=["ch_num"])
        self.update_db(df , db_name)

    def download_tempjson(self , name_db):

        sql_command = f"SELECT * FROM {name_db} ORDER BY Ch"
        cursor = self.getCursor()
        self.onewayLock(cursor=cursor , sql_command=sql_command)
        rows = cursor.fetchall()

        processdata = []
        for row in rows:
            time = None
            if isinstance(row[2] , datetime.datetime):
                time = row[2].strftime("%H:%M:%S")
            LogHandler.logger.info(msg=f"updated time: {time}")

            unit_dict: Dict = {
                'Ch': row[0] ,
                'Name': row[1] ,
                'Time': time ,
                'Voltage': row[3] ,
                'Ohmicx': row[4] ,
                'Ohmicy': row[5] ,
                "Electrode1x": row[6] ,
                "Electrode1y": row[7] ,
                "Electrode2x": row[8] ,
                "Electrode2y": row[9] ,
                "Ohmic_CP": row[10] ,
                "Electrode1_CP": row[11] ,
                "Electrode2_CP": row[12]
            }
            processdata.append(unit_dict)

        return processdata
