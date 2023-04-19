import datetime
import time


class TimeProcess:
    @classmethod
    def getTimestampNow(self) -> datetime.datetime:
        return datetime.datetime.now()

    @classmethod
    def cvtFormatString(cls , dt: datetime , format="%Y-%m-%dT%H:%M:%S"):
        return dt.strftime(format)

    @classmethod
    def getDiffUnixEpochTime(cls , t1: float , t2: float) -> int:
        return t1 - t2 if t1 > t2 else t2 - t1

    @classmethod
    def getDiffDatetime(cls , t1: datetime.datetime , t2: datetime.datetime) -> float:
        diff_dt: datetime.timedelta = (t1 - t2)
        result: float = diff_dt.total_seconds()

        return result
