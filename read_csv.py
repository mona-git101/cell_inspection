import os
import pandas as pd
from utils import read_data_bza_txt

src_path='C:/Users/user/OneDrive/연구업무/202302_셀라인자동화/실험결과/Democsv_0328/Skon_fault/S4C3_1_221_1-1-1_EIS.txt'
data=read_data_bza_txt(src_path)

print(data)   