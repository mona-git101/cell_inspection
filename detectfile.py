import time
from collections import deque
from typing import Dict , Union , List , Tuple
import numpy as np
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import watchdog

from dbutils import *
from logHandler import LogHandler
from inspection import inspect
from timeProcess import TimeProcess
from utils import *


class RealtimeMonitoring:

    # watchDir에 감시하려는 디렉토리를 명시한다.

    def __init__(self , directory: str , ch_num: str , total_ch_num: int):  # 추후에 id, pw, ip 포함하여 init 변경
        self.observer: Observer = Observer()  # observer객체를 만듦
        self.watchDir: str = directory
        self.ch_num: str = ch_num
        self.total_ch_num: int = total_ch_num
        self.run()

    def __call__(self , *args: Tuple , **kwargs: Dict):
        LogHandler.logger.info(msg=f"RealTimeMonitoring 클래스 인스턴스 호출 {args}, {kwargs}, {self}")

    def __str__(self):
        return f"{self.observer}, {self.watchDir}, {self.ch_num}, {self.total_ch_num}"

    def __exit__(self , exc_type , exc_val , exc_tb):
        LogHandler.logger.info(msg=f'모든 스케줄러를 종료합니다...')
        self.observer.unschedule_all()

    def run(self):
        try:
            event_handler: Handler = Handler(
                ch_num=self.ch_num ,
                total_ch_num=self.total_ch_num
            )
            self.observer.schedule(event_handler=event_handler , path=self.watchDir , recursive=True)
            self.observer.start()
            self.observer.join()

        except Exception as ex:
            LogHandler.logger.error(msg=f'{self.ch_num} watchdog EventHandelr를 생성하던 중 오류({ex}) 가 발생했습니다.')


class Meta:
    def __init__(self):
        self.calibration_parameter: Dict[str , int] = {
            'num_reference_training': 10 ,
            'num_target_training': 10
        }
        self.diagnosis_parameter: Dict[str , Union[int , float]] = {
            "num_training": 10 ,
            "statistical_threshold": 3.5 ,
            "safety_factor": 1 ,
            "determinant_limit": 10 ^ -5 ,
            "ohmic_threshold": 10 ^ -3 ,
            "num_calibration_error_check": 10 ,
            "calibration_error_percent_threshold": 5
        }

        self.delta_time_limit: int = 8

        # 초기값 설정
        self.cal_reference: List = []
        self.cal_targets: List = []

        self.idx: int = 1

        self.calibrates: List = []
        self.need_calibration: bool = False
        self.cal_model_exist: bool = False
        self.dia_model_exist: bool = False
        self.training_data: List = []
        self.training_data_length: int = 1000

        self.ref: List = []
        self.diamodel_no: int = 0
        self.no_cal_train: int = 0  # target training 과 연동
        self.no_dia_train: int = 0  # reference training 과 연동
        self.vol_max: float = 3.84  # GUI와 연동 필요함
        self.vol_min: float = 3.80  # GUI와 연동 필요함
        self.jsondict_default: List = []
        self.name_json_db: str = "sendjson"
        self.current_time: datetime.datetime = TimeProcess.getTimestampNow()
        self.current_day_str: str = TimeProcess.cvtFormatString(dt=self.current_time , format='%Y%m%d')
        self.json_day_idx: int = 1

    def validation_calmodel(self , calmodel: Tuple[int , datetime.datetime , float , float , float , float , float , float] , ref: List[Dict]):
        """

        :param calmodel:
        :param ref:
        :return:
        """
        self.calmodel_exist: bool = False
        self.calmodel_no: int = 0

        self.calmodel: Dict[str , List] = {
            'zre': [] ,
            '-zim': []
        }

        self.ref: Dict[str , List] = {
            'zre': [] ,
            '-zim': []
        }

        if len(ref) > 0:
            self.calmodel_exist = True
            self.calmodel_no = calmodel[0]

            self.calmodel = {
                'zre': [calmodel[2] , calmodel[3] , calmodel[4]] ,
                '-zim': [calmodel[5] , calmodel[6] , calmodel[7]]
            }

            self.ref = {
                'zre': [ref[1] , ref[2] , ref[3]] ,
                '-zim': [ref[4] , ref[5] , ref[6]]
            }

            LogHandler.logger.info(msg="calmodel download success")

        else:
            LogHandler.logger.warning(msg="A model for calibration doesn't exist")

    def validation_diamodel(self , diamodel: Tuple[int , datetime.datetime , float , float , float , float , float , float , float , float , float , float , float , float]):
        """

        :param diamodel:
        :return:
        """
        self.diamodel_exist: bool = False
        self.diamodel_no: int = 0
        self.diamodel: Dict[str , Dict] = {
            'p1': {} ,
            'p2': {} ,
            'p3': {} ,
        }

        if len(diamodel) > 0:
            self.diamodel_exist = True
            self.diamodel.update(**{
                'p1': {
                    'learned_mean': diamodel[2] ,
                    'learned_std': diamodel[3]
                } ,
                'p2': {
                    'learned_mean_zre': diamodel[4] ,
                    'learned_mean_-zim': diamodel[5] ,
                    'learned_cov': np.array([[diamodel[6] , diamodel[7]] , [diamodel[7] , diamodel[8]]])
                } ,
                'p3': {
                    'learned_mean_zre': diamodel[9] ,
                    'learned_mean_-zim': diamodel[10] ,
                    'learned_cov': np.array([[diamodel[11] , diamodel[12]] , [diamodel[12] , diamodel[13]]])
                }
            })

            self.diamodel_no = diamodel[0]
            LogHandler.logger.info(msg="diamodel download success")

        else:
            LogHandler.logger.warning(msg="A model for diagnosis doesn't exist")


class Handler(FileSystemEventHandler , Meta):
    def __init__(self , ch_num: str , total_ch_num: int):
        # try:
        LogHandler.logger.info(msg=f'Handler {ch_num}/{total_ch_num}에 대한 모니터링을 시작합니다.')

        super().__init__()  # FileSystemEventHandler 및 Meta 클래스 내 메소드 및 변수들 상속

        self.ch_num: str = ch_num  # ch_num은 폴더명과 동일 ('1', '2', CH3, CH4)
        self.total_ch_num: int = total_ch_num

        self.buffer = deque(maxlen=self.diagnosis_parameter["num_calibration_error_check"])
        self.historical_alarm = deque(maxlen=self.diagnosis_parameter["num_calibration_error_check"])
        self.inspector: inspect = inspect(self.ch_num , self.calibration_parameter , self.diagnosis_parameter)

        self.database: dbconnect = dbconnect()  ## self.idx DB 조회 후 갱신
        self.idx = self.database.latest_idx(ch_num=self.ch_num , pos_idx=2)

        # DB 정보 조회
        calmodel: Tuple[int , datetime.datetime , float , float , float , float , float , float] = self.database.select_model(name_db='calmodel' , ch_num_str=self.ch_num , no='latest')  # 모델 다운로드 후 reference 등에 추가
        diamodel: Tuple[int , datetime.datetime , float , float , float , float , float , float , float , float , float , float , float , float] = self.database.select_model(name_db='diamodel' , ch_num_str=self.ch_num ,
                                                                                                                                                                              no='latest')  ##수정필요

        latest_feature: Tuple = self.database.select_arb_data(name_db='feature' , ch_num_str=self.ch_num , data_num=self.calibration_parameter['num_target_training'] , idx_name='idx')
        ref: List[Dict] = self.database.select_latest_data(name_db='ref' , ch_num=self.ch_num , data_num='all' , offset=1)
        latest_eis: List[Dict] = self.database.select_latest_data(name_db='raweis' , ch_num=self.ch_num , data_num=self.calibration_parameter['num_target_training'] , offset=3)  # target training 만큼 데이터 DB에서 호출

        [self.buffer.append(eis) for eis in latest_eis]  # list compression을 활용한 buffer queue append
        [self.historical_alarm.append(feature[14]) for feature in latest_feature]  # list compression을 활용한 historical_alarm 추가

        self.validation_calmodel(calmodel=calmodel , ref=ref)
        self.validation_diamodel(diamodel=diamodel)

    # except Exception as ex:
    #     ErrorHandler.logger.error(msg=ex)

    def on_created(self , event: watchdog.events.FileCreatedEvent):  # 파일, 디렉터리가 생성되면 실행
        """
        :param event: watchdog 콜백 이벤트 핸들러 (파일 생성 이벤트)
        :return:
        """

        try:
            self.database = dbconnect()
            LogHandler.logger.info(msg=f'db connection successfully. {self.database}')

        except Exception as ex:
            self.database = dbconnect()
            LogHandler.logger.error(msg=f'db connection error. reconnection db ... {ex}')

        src_path = event.src_path.replace('\\' , '/')

        if event.src_path.endswith('.txt'):
            LogHandler.logger.info(msg=f'called callback function(FileCreatedEvent) Txt Reading... {src_path}')

            filename , date , ch = get_fileInfo_idv(directory=src_path)
            if ch[2] == self.ch_num:
                self.idx += 1  # 새로운 파일 입력시 idx 증가
                self.current_time = TimeProcess.getTimestampNow()  # 현재시각 측정

                # measure: Dict = read_data(file=src_path)  # 데이터 입력
                measure: Dict = read_data_bza_txt_long(file=src_path)

                self.buffer.append(measure)  # buffer에 데이터 추가
                self.database.upload_eis(ch_num=self.ch_num , datetime_input=self.current_time , idx=self.idx , measure=measure)  # raweis DB 전송

                # Logic 1: diamodel, calmodel 둘 다 존재 (diamodel이 calmodel보다 선행되어야 하는 학습)
                if self.diamodel_exist:

                    if self.calmodel_exist:  # 두 model 모두 있을 경우 calibration 진행
                        historical_alarm = np.array(self.historical_alarm)
                        calibrated , input_features , diag_result = self.inspector.inspection(input=measure , historical_alarm=historical_alarm , calibration_model=self.calmodel , diagnosis_model=self.diamodel)
                        self.historical_alarm = np.append(self.historical_alarm , diag_result['diagnosis_alarm'])
                        key = self.database.upload_feature(ch_num=self.ch_num , datetime_input=self.current_time , idx=self.idx , voltage=measure['voltage'] , vol_max=self.vol_max , vol_min=self.vol_min ,
                                                           calibrated=calibrated , diag_result=diag_result)  # feature DB 전송

                        prev_json_db = self.database.select_arb_data(name_db='sendjson' , ch_num_str='' , data_num=self.total_ch_num , idx_name='Ch')
                        LogHandler.logger.info(msg=f"prev_json_db: {prev_json_db}")

                        processdataName = key  # 임시로 지정

                        if prev_json_db == ():
                            LogHandler.logger.error(msg=f"empty json db")
                            self.database.upload_tempjson(self.name_json_db , self.ch_num , key , self.current_time , measure , calibrated , diag_result)  # 임시 json db 업데이트

                        else:

                            datetime_measure: List = []
                            ch_measure: List = []

                            for row in prev_json_db:
                                datetime_measure.append(row[2])
                                ch_measure.append(row[0])

                            datetime_measure_min = min(datetime_measure)

                            ch_overlap = ch_measure.count(int(self.ch_num))
                            if self.current_time > datetime_measure_min + datetime.timedelta(seconds=self.delta_time_limit):  # 시간 초과 된 경우
                                LogHandler.logger.warning(msg=f"timeout")

                                ## 이전 DB api로 전송 알고리즘
                                processdata: List[Dict] = self.database.download_tempjson(name_db=self.name_json_db)

                                # 데이터 전송 속도 확인
                                begin_sendTimestamp = TimeProcess.getTimestampNow()
                                (is_ret , response) = sendData(processdata=processdata , processdataName=processdataName)

                                if is_ret:
                                    end_sendTimestamp: datetime.datetime = TimeProcess.getTimestampNow()
                                    sendProcess_tm: float = TimeProcess.getDiffDatetime(t1=end_sendTimestamp , t2=begin_sendTimestamp)
                                    LogHandler.logger.info(msg=f"sendData1 전송속도 체크 : {sendProcess_tm}")

                                    ## DB truncate
                                    self.database.truncate_db(name_db=self.name_json_db)
                                    self.database.upload_tempjson(db_name=self.name_json_db , ch_num=self.ch_num , key=key , datetime_input=self.current_time , measure=measure , calibrated=calibrated ,
                                                                  diag_result=diag_result)
                                else:
                                    LogHandler.logger.error(msg=f"sendData1 {processdataName}를 서버로 전송하지 못했습니다.")

                            elif ch_overlap >= 1:  # 
                                LogHandler.logger.warning(msg=f"incomplete data")

                                ## 이전 DB api로 전송 알고리즘
                                processdata = self.database.download_tempjson(self.name_json_db)

                                begin_sendTimestamp = TimeProcess.getTimestampNow()
                                (is_ret , response) = sendData(processdata , processdataName)

                                if is_ret:
                                    end_sendTimestamp: datetime.datetime = TimeProcess.getTimestampNow()
                                    sendProcess_tm: float = TimeProcess.getDiffDatetime(t1=end_sendTimestamp , t2=begin_sendTimestamp)
                                    LogHandler.logger.info(msg=f"sendData2 전송속도 체크 : {sendProcess_tm}")

                                    self.database.truncate_db(name_db=self.name_json_db)
                                    self.database.upload_tempjson(db_name=self.name_json_db , ch_num=self.ch_num , key=key , datetime_input=self.current_time , measure=measure , calibrated=calibrated ,
                                                                  diag_result=diag_result)
                                else:
                                    LogHandler.logger.error(msg=f"sendData2 {processdataName}를 서버로 전송하지 못했습니다.")

                            else:
                                self.database.upload_tempjson(db_name=self.name_json_db , ch_num=self.ch_num , key=key , datetime_input=self.current_time , measure=measure , calibrated=calibrated ,
                                                              diag_result=diag_result)
                                # self.database = dbconnect()
                                if len(ch_measure) == self.total_ch_num:
                                    LogHandler.logger.warning(msg=f"measurement complete")
                                    processdata = self.database.download_tempjson(name_db=self.name_json_db)

                                    begin_sendTimestamp = TimeProcess.getTimestampNow()
                                    (is_ret , response) = sendData(processdata=processdata , processdataName=processdataName)

                                    if is_ret:
                                        # 전송이 성공하면 아래와 같이 전송 속도를 체크한다.
                                        self.database.truncate_db(name_db=self.name_json_db)
                                        end_sendTimestamp: datetime.datetime = TimeProcess.getTimestampNow()
                                        sendProcess_tm: float = TimeProcess.getDiffDatetime(t1=end_sendTimestamp , t2=begin_sendTimestamp)
                                        LogHandler.logger.info(msg=f"sendData3 전송속도 체크 : {sendProcess_tm}")

                                    else:
                                        LogHandler.logger.error(msg=f"sendData3 {processdataName}를 서버로 전송하지 못했습니다.")

                        # Logic 1-1: calibration alarm 발생시 cal_model 재학습
                        if diag_result['calibration_alarm'] == 1:
                            self.calmodel , self.ref_avg = self.inspector.calibration_training(self.ref , self.buffer , self.calibration_parameter)
                            self.database.upload_calmodel(ch_num=self.ch_num , no=self.calmodel_no + 1 , datetime_input=self.current_time , calmodel=self.calmodel)  # calmodel upload

                    # Logic 2 calmodel만 없을시
                    else:
                        LogHandler.logger.error(msg="No model for calibration")

                else:
                    LogHandler.logger.error(msg="No model for diagnosis")

            # Cal 2. 기존 calibration model이 없거나 buffer data가 부족한 경우
            #
            # Cal 3. 기존 calibration model, buffer data가 존재
