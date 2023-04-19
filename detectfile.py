import os
import time
import pandas as pd
import numpy as np
from collections import defaultdict, deque
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import datetime

from inspection import inspect, diagnose, calibrate
from dbutils import *
from utils import *



class RealtimeMonitoring:
    
    #watchDir에 감시하려는 디렉토리를 명시한다.

    def __init__(self, directory, ch_num, total_ch_num): # 추후에 id, pw, ip 포함하여 init 변경
        self.observer = Observer()   #observer객체를 만듦
        self.watchDir=directory
        self.ch_num=ch_num
        self.total_ch_num=total_ch_num

    def run(self):
        event_handler = Handler(self.ch_num, self.total_ch_num)
        self.observer.schedule(event_handler, self.watchDir, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except:
            self.observer.stop()
            print("Error")
            self.observer.join()
            
class Handler(FileSystemEventHandler):
#FileSystemEventHandler 클래스를 상속받음.
#아래 핸들러들을 오버라이드 함
    def __init__(self, ch_num, total_ch_num):
        # setting 값
        self.ch_num=ch_num #ch_num은 폴더명과 동일 ('1', '2', CH3, CH4)
        self.total_ch_num=total_ch_num
        self.calibration_parameter = {
            'num_reference_training': 10, 
            'num_target_training': 10 
        }
        self.diagnosis_parameter = {
            "num_training": 10,
            "statistical_threshold": 3.5,
            "safety_factor": 1,
            "determinant_limit": 10^-5,
            "ohmic_threshold": 10^-3,
            "num_calibration_error_check": 10,
            "calibration_error_percent_threshold": 5
        }
        self.delta_time_limit=8
        # 초기값 설정
        self.cal_reference = []
        self.cal_targets = []
        self.buffer = deque(maxlen=self.diagnosis_parameter["num_calibration_error_check"])
        self.idx = 1
        self.database = dbconnect()
        self.calibrates = []
        self.need_calibration=False
        self.cal_model_exist = False
        self.dia_model_exist = False
        self.training_data=[]
        self.training_data_length = 1000
        self.inspector = inspect(self.ch_num, self.calibration_parameter, self.diagnosis_parameter)
        self.historical_alarm = deque(maxlen=self.diagnosis_parameter["num_calibration_error_check"])
        self.ref = []
        self.diamodel_no=0
        self.no_cal_train=0 # target training 과 연동
        self.no_dia_train=0 # reference training 과 연동
        self.vol_max=3.84 # GUI와 연동 필요함
        self.vol_min=3.80 # GUI와 연동 필요함
        self.jsondict_default=[]
        self.name_json_db='sendjson'
        self.current_time=datetime.datetime.now()
        self.current_day_str=self.current_time.strftime("%Y%m%d")
        self.json_day_idx=1

        ## self.idx DB 조회 후 갱신
        latest_idx=self.database.latest_idx(self.ch_num, 2) 
        self.idx=latest_idx

        # DB 정보 조회
        calmodel = self.database.select_model('calmodel', self.ch_num,'latest') # 모델 다운로드 후 reference 등에 추가 
        ref=self.database.select_latest_data('ref',self.ch_num, 'all',1)
        latest_eis=self.database.select_latest_data('raweis', self.ch_num, self.calibration_parameter['num_target_training'],3) #target training 만큼 데이터 DB에서 호출
        diamodel = self.database.select_model('diamodel',self.ch_num,'latest') ##수정필요
        latest_feature = self.database.select_arb_data('feature', self.ch_num, self.calibration_parameter['num_target_training'], 'idx')
        # buffer에 데이터 추가
        for eis in latest_eis:
            self.buffer.append(eis)
        # historical_alarm 추가
        for feature in latest_feature:
            self.historical_alarm.append(feature[14])

        # cal_model 및 ref 입력
        if calmodel == []:
            self.calmodel_exist = False
            self.calmodel_no=0
            print("A model for calibration doesn't exist")
        else: # Calibration training 필요
            if ref == []:
                self.calmodel_exist = False  
            else:
                self.calmodel_exist = True
                self.calmodel = {
                    'zre' : [calmodel[2], calmodel[3], calmodel[4]],
                    '-zim' : [calmodel[5], calmodel[6], calmodel[7]]
                }
                self.calmodel_no=calmodel[0]
                self.ref = {
                    'zre' : [ref[1], ref[2], ref[3]],
                    '-zim' : [ref[4], ref[5], ref[6]]
                }
            print("model download success")

        # dia_model 입력
        if diamodel == []:
            self.diamodel_exist = False
            self.diamodel_no=0
            print("A model for diagnosis doesn't exist")
        else:        
            self.diamodel_exist = True
            self.diamodel={}
            self.diamodel['p1'] = {
                'learned_mean': diamodel[2],
                'learned_std': diamodel[3]
            }
            self.diamodel['p2'] = {
                'learned_mean_zre': diamodel[4],
                'learned_mean_-zim': diamodel[5],
                'learned_cov': np.array([[diamodel[6], diamodel[7]], [diamodel[7], diamodel[8]]])
            }
            self.diamodel['p3'] = {
                'learned_mean_zre': diamodel[9],
                'learned_mean_-zim': diamodel[10],
                'learned_cov': np.array([[diamodel[11], diamodel[12]], [diamodel[12], diamodel[13]]])
            }
            self.diamodel_no=diamodel[0]
            print("model download success")
        
        
        


    def on_created(self, event): # 파일, 디렉터리가 생성되면 실행
        self.database = dbconnect()

        src_path=event.src_path.replace('\\', '/')

        print(event)
        if event.src_path[-3:] == 'csv':
            print("csv reading...", src_path)
            filename, date, ch = get_fileInfo_idv(src_path)
            if ch[2] == self.ch_num:
                self.idx += 1  # 새로운 파일 입력시 idx 증가
                self.current_time=datetime.datetime.now() #현재시각 측정
                measure=read_data(src_path) # 데이터 입력                

                self.buffer.append(measure) #buffer에 데이터 추가
                self.database.upload_eis(self.ch_num, self.current_time, self.idx, measure) #raweis DB 전송
                                
                # Logic 1: diamodel, calmodel 둘 다 존재 (diamodel이 calmodel보다 선행되어야 하는 학습)
                if self.diamodel_exist:
                    if self.calmodel_exist: # 두 model 모두 있을 경우 calibration 진행
                        historical_alarm=np.array(self.historical_alarm)
                        calibrated, input_features, diag_result = self.inspector.inspection(measure, historical_alarm, self.calmodel, self.diamodel)
                        self.historical_alarm=np.append(self.historical_alarm, diag_result['diagnosis_alarm'])
                        key= self.database.upload_feature(self.ch_num, self.current_time, self.idx, measure['voltage'], self.vol_max, self.vol_min, calibrated, diag_result) #feature DB 전송
                        prev_json_db=self.database.select_arb_data('sendjson','', self.total_ch_num, 'Ch')
                        print('prev_json_db:', prev_json_db)
                        # 
                        processdataName=key #임시로 지정
                        if prev_json_db == ():
                            print('empty json db')
                            self.database.upload_tempjson(self.name_json_db, self.ch_num, key, self.current_time, measure, calibrated, diag_result) # 임시 json db 업데이트
                        else:
                            datetime_measure=[]
                            ch_measure=[]
                            for row in prev_json_db:
                                datetime_measure.append(row[2])
                                ch_measure.append(row[0])
                            datetime_measure_min=min(datetime_measure)
                            ch_overlap = ch_measure.count(int(self.ch_num))
                            if self.current_time > datetime_measure_min + datetime.timedelta(seconds=self.delta_time_limit): # 시간 초과 된 경우
                                ## 이전 DB api로 전송 알고리즘
                                print('warning: timeout')
                                processdata=self.database.download_tempjson(self.name_json_db)
                                res = sendData(processdata, processdataName)
                                ## DB truncate
                                self.database.truncate_db(self.name_json_db)
                                ##
                                self.database.upload_tempjson(self.name_json_db, self.ch_num, key, self.current_time, measure, calibrated, diag_result)
                            elif ch_overlap >= 1:  # 
                                print('warning: incomplete data')
                                ## 이전 DB api로 전송 알고리즘
                                processdata=self.database.download_tempjson(self.name_json_db)
                                res = sendData(processdata, processdataName)
                                ## DB truncate
                                self.database.truncate_db(self.name_json_db)
                                ##
                                self.database.upload_tempjson(self.name_json_db, self.ch_num, key, self.current_time, measure, calibrated, diag_result)
                            else:
                                self.database.upload_tempjson(self.name_json_db, self.ch_num, key, self.current_time, measure, calibrated, diag_result)
                                self.database = dbconnect()
                                if len(ch_measure) == self.total_ch_num:
                                    print('measurement complete')
                                    processdata=self.database.download_tempjson(self.name_json_db)
                                    res = sendData(processdata, processdataName)
                                    self.database.truncate_db(self.name_json_db)
                        
                        # Logic 1-1: calibration alarm 발생시 cal_model 재학습
                        if diag_result['calibration_alarm'] == 1:
                            self.calmodel, self.ref_avg = self.inspector.calibration_training(self.ref, self.buffer, self.calibration_parameter)
                            self.database.upload_calmodel(self.ch_num, self.calmodel_no+1, self.current_time, self.calmodel) # calmodel upload
                    # Logic 2 calmodel만 없을시
                    else:
                        print("No model for calibration")  

                else:
                    print("No model for diagnosis")

            # Cal 2. 기존 calibration model이 없거나 buffer data가 부족한 경우
            #
            # Cal 3. 기존 calibration model, buffer data가 존재
        





