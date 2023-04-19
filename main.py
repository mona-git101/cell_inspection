from multiprocessing.pool import ApplyResult
from typing import List
from logHandler import LogHandler
from detectfile import RealtimeMonitoring
from multiProcessing import MultiProcessing

# 세팅값 불러오기 (from criterion_data)
setting_dir = 'data/criterion_data/reference'
setting_file_name = 'info.csv'
setting_file_path = f'{setting_dir}/{setting_file_name}'

root: str = f'data/raw_data'
root_dir = root_dir = f'{root}'
total_ch_num: int = 2

if __name__ == '__main__':
    LogHandler.generate_log_folder(path=LogHandler.LOG_FOLDER)
    LogHandler.addFileHandler()
    LogHandler.addStreamHandler()

    asyncMultiProcessor: MultiProcessing = MultiProcessing(pool_size=total_ch_num , is_thread=True)
    applyAsyncResult: List[ApplyResult] = [asyncMultiProcessor.signApplyResultJob(
        func=RealtimeMonitoring , args=(root_dir , str(i) , total_ch_num)) for i in range(1 , total_ch_num + 1)
    ]

    # timeout에서 원하는만큼 시간을 제어할 수 있습니다 (None: 무한)
    timeout = None
    asyncMultiProcessor.applyAsyncStartRoutine(applyAsync_List=applyAsyncResult , timeout=timeout , output=True)

    # threads = []
    # for i in range(1 , total_ch_num + 1):
    #     setting_thread = RealtimeMonitoring(root_dir , str(i) , total_ch_num)
    #     print(setting_thread)
    #     print('Thread: ' , i)
    #
    #     t = threading.Thread(target=setting_thread.run , args=())
    #     t.start()
    #     threads.append(t)
    #
    #     break  # TODO Remove
