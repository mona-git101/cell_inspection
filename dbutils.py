import pymysql
from sqlalchemy import create_engine
import pandas as pd
import datetime
from array import *
import numpy as np

class dbconnect (object):
    
    def __init__(self):
        self.engine = create_engine("mysql+pymysql://root:"+"mona1101!!"+"@127.0.0.1:3306/cei_dev1?charset=utf8", encoding='utf-8')
        self.conn = self.engine.connect()
        self.conn_raw = self.engine.raw_connection()
        self.cur=self.conn_raw.cursor()

    def update_db(self, df, name_db):
        df.to_sql(name_db, con=self.engine, if_exists='append', index=False)
        self.conn.close()
        #except:
        #    print("update_db error")

    def truncate_db(self, name_db):
        sql_command = 'truncate table ' + name_db
        self.cur.execute(sql_command)


    def upload_calmodel(self, ch_num, no, datetime_input, calmodel) :
        # no의 경우 이전 모델을 불러올 때 db에서 정보 호출
        # name_db = calmodel, diamodel
        # no: model number = 1,2,3,..
        # date: 모델 생성 날짜 -> datetime으로 변경 필요
        tbl_name = 'calmodel' + str(ch_num)
        raw_data={'no':[no], 'datetime':[datetime_input], 'zre1':calmodel['zre'][0], 'zre2':calmodel['zre'][1], 'zre3':calmodel['zre'][2], 'zim1':calmodel['-zim'][0], 'zim2':calmodel['-zim'][1], 'zim3':calmodel['-zim'][2]}
        df=pd.DataFrame(raw_data)
        self.update_db(df, tbl_name)

        

    def upload_eis(self, ch_num, datetime_input, idx, measure):
        # date 형식: string
        print('uploading eis')
        tbl_name='raweis' + str(ch_num)
        if idx<10:
            str_idx='000'+str(idx)
        elif idx<100:
            str_idx='00'+str(idx)
        elif idx<1000:
            str_idx='0'+str(idx)
        elif idx<10000:
            str_idx=str(idx)
        else:
            print('error, idx should be lower than 10000')
        key=str(datetime_input.year)
        if datetime_input.month <10:
            key = key + '0' + str(datetime_input.month)
        else:
            key = key + str(datetime_input.month)
        if datetime_input.day <10:
            key = key + '0' + str(datetime_input.day) 
        else:
            key = key + str(datetime_input.day)
        key = key + str(ch_num) + str_idx
        raw_data={'key':[key], 'datetime':[datetime_input], 'idx':[idx], 'zre1':measure['zre'][0], 'zre2':measure['zre'][1], 'zre3':measure['zre'][2], 'zim1':measure['-zim'][0], 'zim2':measure['-zim'][1], 'zim3':measure['-zim'][2]}
        df=pd.DataFrame(raw_data)
        self.update_db(df, tbl_name)

    def upload_feature(self, ch_num, datetime_input, idx, voltage, vol_max, vol_min, calibrated, diag_result):
        # date 형식: string
        print('uploading feature')
        tbl_name='feature' + str(ch_num)
        if idx<10:
            str_idx='000'+str(idx)
        elif idx<100:
            str_idx='00'+str(idx)
        elif idx<1000:
            str_idx='0'+str(idx)
        elif idx<10000:
            str_idx=str(idx)
        else:
            print('error, idx should be lower than 10000')

        key=str(datetime_input.year)
        if datetime_input.month <10:
            key = key + '0' + str(datetime_input.month)
        else:
            key = key + str(datetime_input.month)
        if datetime_input.day <10:
            key = key + '0' + str(datetime_input.day) 
        else:
            key = key + str(datetime_input.day)
        key = key + str(ch_num) + str_idx

        if voltage > vol_max:
            vol_alarm=1
        elif voltage < vol_min:
            vol_alarm=1
        else:
            vol_alarm = 0
        status = vol_alarm + diag_result['diagnosis_alarm'] - diag_result['diagnosis_alarm']*vol_alarm
        raw_data={
            'key':[key],
            'datetime':[datetime_input],
            'idx':[idx],
            'voltage':voltage,
            'zre1cal': calibrated['zre'][0],
            'zre2cal': calibrated['zre'][1],
            'zre3cal': calibrated['zre'][2],
            'zim1cal': calibrated['-zim'][0],
            'zim2cal': calibrated['-zim'][1],
            'zim3cal': calibrated['-zim'][2],
            'stadist1': diag_result['statistical_distances'][0],
            'stadist2': diag_result['statistical_distances'][1],
            'stadist3': diag_result['statistical_distances'][2],
            'status':status,
            'diaalarm': diag_result['diagnosis_alarm'],
            'diaalarmidx1': diag_result['diagnosis_alarm_index'][0],
            'diaalarmidx2': diag_result['diagnosis_alarm_index'][1],
            'diaalarmidx3': diag_result['diagnosis_alarm_index'][2],
            'volalarm':vol_alarm,
            'calalarm': diag_result['calibration_alarm'],
            'ohmalarm': diag_result['ohmic_alarm']
            }
        df=pd.DataFrame(raw_data)
        self.update_db(df, tbl_name)
        return key

    def select_model(self, name_db, ch_num_str, no):
        tbl_name=name_db + ch_num_str
        if no == 'latest':
            sql_command = "select * from "+tbl_name+" order by no desc limit 1" ## 수정 필요 (동작안함)
        else: 
            sql_command = "select * from "+tbl_name+" where no like " + str(no)
        self.cur.execute(sql_command)
        row=self.cur.fetchone()
        return row # tuple 형태로 출력?

    def select_arb_data(self, name_db, ch_num_str, data_num, idx_name):
        tbl_name = name_db + ch_num_str
        if data_num == 'all':
            sql_command = "SELECT * FROM " + tbl_name + " limit 0, 4"
        elif data_num == 'latest':
            sql_command = "select * from "+tbl_name+" order by " + idx_name + " desc limit 1"
        else:
            sql_command ="SELECT * FROM " + tbl_name + " ORDER BY "+ idx_name + " desc limit " + str(data_num)
        self.cur.execute(sql_command)
        result = self.cur.fetchall()
        print('result:', result)

        return result


    def select_latest_data(self, name_db, ch_num, data_num, offset):
        tbl_name = name_db + str(ch_num)
        if data_num == 'all':
            sql_command = "SELECT * FROM " + tbl_name + " ORDER BY idx"
        else:
            sql_command ="SELECT * FROM " + tbl_name + " ORDER BY idx LIMIT " + str(data_num)
        self.cur.execute(sql_command)
        rows=self.cur.fetchall()
        measures_db=[]
        for row in rows: #### 수정u
            zre=np.array([row[offset], row[offset+1], row[offset+2]])
            zim=np.array([row[offset+3], row[offset+4], row[offset+5]])
            eis_data={'zre':zre, '-zim':-zim}
            measures_db.append(eis_data) 
        return measures_db

    def latest_idx(self, ch_num, pos_idx):
        tbl_name='raweis' + str(ch_num)
        sql_command="select * from "+tbl_name+" order by idx desc limit 1"
        self.cur.execute(sql_command)
        rows=self.cur.fetchall()
        row=rows[0]
        idx=row[pos_idx]
        return idx

    def upload_tempjson(self, db_name, ch_num, key, datetime_input, measure, calibrated, diag_result):
        tbl_name=db_name
        
        data={
            'Ch': ch_num,
            'Name': key,
            'Time': datetime_input,
            'Voltage': measure['voltage'],
            'Ohmicx': calibrated['zre'][0],
            'Ohmicy': calibrated['-zim'][0],
            "Electrode1x": calibrated['zre'][1],
            "Electrode1y": calibrated['-zim'][1],
            "Electrode2x": calibrated['zre'][2],
            "Electrode2y": calibrated['-zim'][2],
            "Ohmic_CP": diag_result['statistical_distances'][0],
            "Electrode1_CP": diag_result['statistical_distances'][1],
            "Electrode2_CP": diag_result['statistical_distances'][2]
        }
        df=pd.DataFrame(data, index=["ch_num"])
        self.update_db(df, tbl_name)
    
    def download_tempjson(self, name_db):
        sql_command = "SELECT * FROM " + name_db + " ORDER BY Ch"
        self.cur.execute(sql_command)
        rows=self.cur.fetchall()
        processdata=[]
        for row in rows:
            time=row[2]
            print("updated time:", time.strftime("%H:%M:%S"))
            unit_dict={
                'Ch': row[0],
                'Name': row[1],
                'Time': time.strftime("%H:%M:%S"),
                'Voltage': row[3],
                'Ohmicx': row[4],
                'Ohmicy': row[5],
                "Electrode1x": row[6],
                "Electrode1y": row[7],
                "Electrode2x": row[8],
                "Electrode2y": row[9],
                "Ohmic_CP": row[10],
                "Electrode1_CP": row[11],
                "Electrode2_CP": row[12]
            }
            
            processdata.append(unit_dict)
        return processdata

