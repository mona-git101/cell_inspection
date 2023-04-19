import multiprocessing.pool
from multiprocessing.pool import ThreadPool , Pool , ApplyResult
from typing import Tuple , List , Dict , Callable , Union


class MultiProcessing:
    hThreadPool: Union[Pool , ThreadPool] = None
    is_thread: bool = False

    def __init__(self , pool_size: int , is_thread: bool = False):
        if is_thread:
            self.hThreadPool = ThreadPool(
                processes=pool_size
            )
            self.is_thread = True

        else:
            self.hThreadPool = Pool(
                processes=pool_size
            )

    def signApplyResultJob(self , func: Callable , args: Tuple = ()) -> multiprocessing.pool.ApplyResult:
        result_applyResult: ApplyResult = self.hThreadPool.apply_async(func=func , args=args)
        return result_applyResult

    def applyAsyncStartRoutine(self , applyAsync_List: List[ApplyResult] , timeout: int = None , output: bool = False):
        is_start: bool = False
        ret: List[Dict] = []

        if output:
            ret = [ret.append(applyAsync.get(timeout=timeout)) for applyAsync in applyAsync_List]
            is_start = True
        else:
            [applyAsync.get(timeout=timeout) for applyAsync in applyAsync_List]
            is_start = True

        applyAsync_List.clear()
        return is_start , ret
