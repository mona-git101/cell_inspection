import logging
import os.path

from timeProcess import TimeProcess


class LogHandler:
    """
    에러 발생 시 프로그램의 중단 없이 에러코드를 출력하고 프로그램을
    진행하기 위한 에러 처리 핸들러 클래스
    """

    ROOT_DIR: str = os.path.join(os.path.abspath('.'))
    _format = logging.Formatter(
        "[%(asctime)s::%(name)s] %(levelname)s  FileName(%(filename)s|%(lineno)d])  메세지([%(message)s])" ,
        datefmt="%Y-%m-%dT%H:%M:%S.%Z" ,
    )
    LOG_FOLDER = f"{ROOT_DIR}/LOG/{TimeProcess.getTimestampNow().strftime('%Y%m%d')}"
    logger = logging.Logger(name='LogHandler' , level=logging.DEBUG)

    @classmethod
    def generate_log_folder(self , path: str):
        if not os.path.isdir(path):
            os.makedirs(name=path , exist_ok=True)

    @classmethod
    def addFileHandler(cls):
        fileHandler = logging.FileHandler(filename=f'{cls.LOG_FOLDER}/PROCESS.LOG' , encoding='utf-8')
        fileHandler.setLevel(level=logging.INFO)
        fileHandler.setFormatter(fmt=cls._format)
        cls.logger.addHandler(hdlr=fileHandler)

    @classmethod
    def addStreamHandler(cls):
        streamHandler = logging.StreamHandler()
        streamHandler.setLevel(level=logging.DEBUG)
        streamHandler.setFormatter(fmt=cls._format)
        cls.logger.addHandler(hdlr=streamHandler)
