# -*- coding: utf-8 -*-
"""
@File    :  ExcelRegenerate.py
@Time    :  2022/4/3 10:05
@Author  :  zhuyugang
@Version :  1.0
@Desc    :  进行时间截取后的Excel表生成
"""
import pandas as pd
import re
class ExcelRegenerate(object):
    def __init__(self, my_model=None, path_time=None, time_first=None, time_last=None, file_addr_new=None):
        self.my_model = my_model
        self.path_time = path_time
        self.time_first = time_first
        self.time_last = time_last
        self.file_addr_new =file_addr_new


    def time_location_first(self, time_first_int, x):
        # 对行函数进行
        x_1 = re.split('[^0-9]+', x)[:-1]
        x_1_int = int(x_1[0]) * (10 ** 10) + int(x_1[1]) * (10 ** 8) + int(x_1[2]) * (10 ** 6) + int(x_1[3]) * (
                10 ** 4) + int(x_1[4]) * (10 ** 2) + int(x_1[5])
        if x_1_int - time_first_int == 0:   # 找到初始点，即第一个与time_first_int相等的时间点；
            return 1
        else:
            return 0
    def time_location_last(self, time_last_int, x):
        x_1 = re.split('[^0-9]+', x)[:-1]
        x_1_int = int(x_1[0]) * (10 ** 10) + int(x_1[1]) * (10 ** 8) + int(x_1[2]) * (10 ** 6) + int(x_1[3]) * (
                    10 ** 4) + int(x_1[4]) * (10 ** 2) + int(x_1[5])
        if x_1_int - time_last_int == 0:    # 找到终止点，time_last_int相等的时间点刚过的第一个时间点；
            return 1
        else:
            return 0
    def time_location(self, time_first_int, time_last_int, x):
        x_1 = re.split('[^0-9]+', x)[:-1]
        if len(x_1) == 6:
            x_1_int = int(x_1[0]) * (10 ** 10) + int(x_1[1]) * (10 ** 8) + int(x_1[2]) * (10 ** 6) + int(x_1[3]) * (
                    10 ** 4) + int(x_1[4]) * (10 ** 2) + int(x_1[5])
            if time_first_int - x_1_int <= 0 and time_last_int - x_1_int >= 0:
                return 1
            else:
                return 0
        else:
            return 0
    def excel_regenerate(self, my_model, path_time, time_first, time_last, file_addr_new):
        global err_ip
        xl = pd.read_excel(path_time, sheet_name='sheet1')
        df = pd.read_excel(path_time, keep_default_na=False)
        row = df.shape[0]  # 获取行数
        empty_exl = []
        if time_first == '' and time_last == '':
            err_ip = 4
            try:
                df.to_excel(file_addr_new, sheet_name='Sheet1', index=False, header=True)
            except Exception as e:
                print(e)
            return err_ip, empty_exl
        if time_first == '':
            pass
        else:
            time_first_1 = re.split('[^0-9]+', time_first)
            time_first_int = int(time_first_1[0])*(10**10)+int(time_first_1[1])*(10**8)+int(time_first_1[2])*(10**6)+int(time_first_1[3])*(10**4)+int(time_first_1[4])*(10**2)+int(time_first_1[5])
        if time_last == '':
            pass
        else:
            time_last_1 = re.split('[^0-9]+', time_last)
            time_last_int = int(time_last_1[0]) * (10 ** 10) + int(time_last_1[1]) * (10 ** 8) + int(time_last_1[2]) * (10 ** 6) +int(time_last_1[3]) * (10 ** 4) + int(time_last_1[4]) * (10 ** 2) + int(time_last_1[5])
        if time_first == '' and time_last != '':
            time_first_int = 0
            err_ip = 1
        elif time_first != '' and time_last == '':
            time_last_int = 99999999999999
            err_ip = 2
        elif time_first != '' and time_last != '':
            err_ip = 3

        if time_last_int - time_first_int < 0:
            err_ip = 0
            print('输入时间大小错误')
            return err_ip, empty_exl

        if my_model == '交大':
            # PRI起止时间点
            df_all_pri = pd.DataFrame()
            time_pri_x_num = 0
            for i in range(row):
                time_pri_x = str(df.loc[i, 'PRI_触发时间_1'])
                if time_pri_x == '' or time_pri_x == ' ':
                    time_pri_x_num = time_pri_x_num + 1
                    continue
                elif time_pri_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_pri_x) == 1:
                        df_list_pri = pd.DataFrame()
                        for j in range(0, 34):
                            df_list_pri = pd.concat([df_list_pri, xl.iloc[[i], [j]]], axis=1)
                        df_all_pri = pd.concat([df_all_pri, df_list_pri], axis=0)
            if df_all_pri.empty:
                for j in range(0, 34):
                    df_all_pri = pd.concat([df_all_pri, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('PRI接口信令无该时间段数据')
                print('PRI接口信令无该时间段数据')

            # PRI_2起止时间点
            df_all_pri_2 = pd.DataFrame()
            time_pri_2_x_num = 0
            for i in range(row):
                time_pri_2_x = str(df.loc[i, 'PRI_起呼时间_2'])
                if time_pri_2_x == '' or time_pri_2_x == ' ':
                    time_pri_2_x_num = time_pri_2_x_num + 1
                    continue
                elif time_pri_2_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_pri_2_x) == 1:
                        df_list_pri_2 = pd.DataFrame()
                        for j in range(34, 57):
                            df_list_pri_2 = pd.concat([df_list_pri_2, xl.iloc[[i], [j]]], axis=1)
                        df_all_pri_2 = pd.concat([df_all_pri_2, df_list_pri_2], axis=0)
            if df_all_pri_2.empty:
                for j in range(34, 57):
                    df_all_pri_2 = pd.concat([df_all_pri_2, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('PRI呼叫记录无该时间段数据')
                print('PRI呼叫记录无该时间段数据')

            # Abis_1起止时间点
            df_all_abis_1 = pd.DataFrame()
            time_abis_1_x_num = 0
            for i in range(row):
                time_abis_1_x = str(df.loc[i, 'Abis_记录时间_1'])
                if time_abis_1_x == '' or time_abis_1_x == ' ':
                    time_abis_1_x_num = time_abis_1_x_num + 1
                    continue
                elif time_abis_1_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_abis_1_x) == 1:
                        df_list_abis_1 = pd.DataFrame()
                        for j in range(57, 87):
                            df_list_abis_1 = pd.concat([df_list_abis_1, xl.iloc[[i], [j]]], axis=1)
                        df_all_abis_1 = pd.concat([df_all_abis_1, df_list_abis_1], axis=0)
            if df_all_abis_1.empty:
                for j in range(57, 87):
                    df_all_abis_1 = pd.concat([df_all_abis_1, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('Abis接口信令无该时间段数据')
                print('Abis接口信令无该时间段数据')

            # Abis_2起止时间点
            df_all_abis_2 = pd.DataFrame()
            time_abis_2_x_num = 0
            for i in range(row):
                time_abis_2_x = str(df.loc[i, 'Abis_触发时间_2'])
                if time_abis_2_x == '' or time_abis_2_x == ' ':
                    time_abis_2_x_num = time_abis_2_x_num + 1
                    continue
                elif time_abis_2_x_num >= 50:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_abis_2_x) == 1:
                        df_list_abis_2 = pd.DataFrame()
                        for j in range(87, 121):
                            df_list_abis_2 = pd.concat([df_list_abis_2, xl.iloc[[i], [j]]], axis=1)
                        df_all_abis_2 = pd.concat([df_all_abis_2, df_list_abis_2], axis=0)
            if df_all_abis_2.empty:
                for j in range(87, 121):
                    df_all_abis_2 = pd.concat([df_all_abis_2, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('Abis测量报告无该时间段数据')
                print('Abis测量报告无该时间段数据')

            # Abis_3起止时间点
            df_all_abis_3 = pd.DataFrame()
            time_abis_3_x_num = 0
            for i in range(row):
                time_abis_3_x = str(df.loc[i, 'Abis_开始时间_3'])
                if time_abis_3_x == '' or time_abis_3_x == ' ':
                    time_abis_3_x_num = time_abis_3_x_num + 1
                    continue
                elif time_abis_3_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_abis_3_x) == 1:
                        df_list_abis_3 = pd.DataFrame()
                        for j in range(121, 152):
                            df_list_abis_3 = pd.concat([df_list_abis_3, xl.iloc[[i], [j]]], axis=1)
                        df_all_abis_3 = pd.concat([df_all_abis_3, df_list_abis_3], axis=0)
            if df_all_abis_3.empty:
                for j in range(121, 152):
                    df_all_abis_3 = pd.concat([df_all_abis_3, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('Abis切换记录无该时间段数据')
                print('Abis切换记录无该时间段数据')

            # Abis_4起止时间点
            df_all_abis_4 = pd.DataFrame()
            time_abis_4_x_num = 0
            for i in range(row):
                time_abis_4_x = str(df.loc[i, 'Abis_起呼时间_4'])
                if time_abis_4_x == '' or time_abis_4_x == ' ':
                    time_abis_4_x_num = time_abis_4_x_num + 1
                    continue
                elif time_abis_4_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_abis_4_x) == 1:
                        df_list_abis_4 = pd.DataFrame()
                        for j in range(152, 175):
                            df_list_abis_4 = pd.concat([df_list_abis_4, xl.iloc[[i], [j]]], axis=1)
                        df_all_abis_4 = pd.concat([df_all_abis_4, df_list_abis_4], axis=0)
            if df_all_abis_4.empty:
                for j in range(152, 175):
                    df_all_abis_4 = pd.concat([df_all_abis_4, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('Abis呼叫记录无该时间段数据')
                print('Abis呼叫记录无该时间段数据')

            # A_1起止时间点
            df_all_a_1 = pd.DataFrame()
            time_a_1_x_num = 0
            for i in range(row):
                time_a_1_x = str(df.loc[i, 'A_记录时间_1'])
                if time_a_1_x == '' or time_a_1_x == ' ':
                    time_a_1_x_num = time_a_1_x_num + 1
                    continue
                elif time_a_1_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_a_1_x) == 1:
                        df_list_a_1 = pd.DataFrame()
                        for j in range(175, 204):
                            df_list_a_1 = pd.concat([df_list_a_1, xl.iloc[[i], [j]]], axis=1)
                        df_all_a_1 = pd.concat([df_all_a_1, df_list_a_1], axis=0)
            if df_all_a_1.empty:
                for j in range(175, 204):
                    df_all_a_1 = pd.concat([df_all_a_1, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('A接口信令无该时间段数据')
                print('A接口信令无该时间段数据')

            # A_2起止时间点
            df_all_a_2 = pd.DataFrame()
            time_a_2_x_num = 0
            for i in range(row):
                time_a_2_x = str(df.loc[i, 'A_开始时间_2'])
                if time_a_2_x == '' or time_a_2_x == ' ':
                    time_a_2_x_num = time_a_2_x_num + 1
                    continue
                elif time_a_2_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_a_2_x) == 1:
                        df_list_a_2 = pd.DataFrame()
                        for j in range(204, 233):
                            df_list_a_2 = pd.concat([df_list_a_2, xl.iloc[[i], [j]]], axis=1)
                        df_all_a_2 = pd.concat([df_all_a_2, df_list_a_2], axis=0)
            if df_all_a_2.empty:
                for j in range(204, 233):
                    df_all_a_2 = pd.concat([df_all_a_2, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('A切换记录无该时间段数据')
                print('A切换记录无该时间段数据')

            # A_3起止时间点
            df_all_a_3 = pd.DataFrame()
            time_a_3_x_num = 0
            for i in range(row):
                time_a_3_x = str(df.loc[i, 'A_起呼时间_3'])
                if time_a_3_x == '' or time_a_3_x == ' ':
                    time_a_3_x_num = time_a_3_x_num + 1
                    continue
                elif time_a_3_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_a_3_x) == 1:
                        df_list_a_3 = pd.DataFrame()
                        for j in range(233, 254):
                            df_list_a_3 = pd.concat([df_list_a_3, xl.iloc[[i], [j]]], axis=1)
                        df_all_a_3 = pd.concat([df_all_a_3, df_list_a_3], axis=0)
            if df_all_a_3.empty:
                for j in range(233, 254):
                    df_all_a_3 = pd.concat([df_all_a_3, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('A呼叫记录无该时间段数据')
                print('A呼叫记录无该时间段数据')
            df_all_pri.to_excel('.\\data\\excel_time\\intermediate\\df_all_pri.xls', sheet_name='Sheet1', index=False,
                                header=True)
            df_all_pri_2.to_excel('.\\data\\excel_time\\intermediate\\df_all_pri_2.xls', sheet_name='Sheet1', index=False,
                                  header=True)
            df_all_abis_1.to_excel('.\\data\\excel_time\\intermediate\\df_all_abis_1.xls', sheet_name='Sheet1', index=False,
                                   header=True)
            df_all_abis_2.to_excel('.\\data\\excel_time\\intermediate\\df_all_abis_2.xls', sheet_name='Sheet1', index=False,
                                   header=True)
            df_all_abis_3.to_excel('.\\data\\excel_time\\intermediate\\df_all_abis_3.xls', sheet_name='Sheet1', index=False,
                                   header=True)
            df_all_abis_4.to_excel('.\\data\\excel_time\\intermediate\\df_all_abis_4.xls', sheet_name='Sheet1', index=False,
                                   header=True)
            df_all_a_1.to_excel('.\\data\\excel_time\\intermediate\\df_all_a_1.xls', sheet_name='Sheet1', index=False,
                                header=True)
            df_all_a_2.to_excel('.\\data\\excel_time\\intermediate\\df_all_a_2.xls', sheet_name='Sheet1', index=False,
                                header=True)
            df_all_a_3.to_excel('.\\data\\excel_time\\intermediate\\df_all_a_3.xls', sheet_name='Sheet1', index=False,
                                header=True)

            xl1 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_pri.xls', sheet_name='Sheet1')
            xl2 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_pri_2.xls', sheet_name='Sheet1')
            xl3 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_abis_1.xls', sheet_name='Sheet1')
            xl4 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_abis_2.xls', sheet_name='Sheet1')
            xl5 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_abis_3.xls', sheet_name='Sheet1')
            xl8 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_abis_4.xls', sheet_name='Sheet1')
            xl6 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_a_1.xls', sheet_name='Sheet1')
            xl7 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_a_2.xls', sheet_name='Sheet1')
            xl9 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_a_3.xls', sheet_name='Sheet1')

            df_all_jiaoda = pd.concat([xl1, xl2, xl3, xl4, xl5, xl8, xl6, xl7, xl9], axis=1)
            df_all_jiaoda.to_excel(file_addr_new, sheet_name='Sheet1', index=False, header=True)

        if my_model == '通号':
            # PRI起止时间点
            df_all_pri = pd.DataFrame()
            time_pri_x_num = 0
            for i in range(row):
                time_pri_x = str(df.loc[i, 'PRI_触发时间_1'])
                if time_pri_x == '' or time_pri_x == ' ':
                    time_pri_x_num = time_pri_x_num + 1
                    continue
                elif time_pri_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_pri_x) == 1:
                        df_list_pri = pd.DataFrame()
                        for j in range(0, 26):
                            df_list_pri = pd.concat([df_list_pri, xl.iloc[[i], [j]]], axis=1)
                        df_all_pri = pd.concat([df_all_pri, df_list_pri], axis=0)
            if df_all_pri.empty:
                for j in range(0, 26):
                    df_all_pri = pd.concat([df_all_pri, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('PRI接口信令无该时间段数据')
                print('PRI接口信令无该时间段数据')

            # PRI_2起止时间点
            df_all_pri_2 = pd.DataFrame()
            time_pri_2_x_num = 0
            for i in range(row):
                time_pri_2_x = str(df.loc[i, 'PRI_发生时间_2'])
                if time_pri_2_x == '' or time_pri_2_x == ' ':
                    time_pri_2_x_num = time_pri_2_x_num + 1
                    continue
                elif time_pri_2_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_pri_2_x) == 1:
                        df_list_pri_2 = pd.DataFrame()
                        for j in range(26, 41):
                            df_list_pri_2 = pd.concat([df_list_pri_2, xl.iloc[[i], [j]]], axis=1)
                        df_all_pri_2 = pd.concat([df_all_pri_2, df_list_pri_2], axis=0)
            if df_all_pri_2.empty:
                for j in range(26, 41):
                    df_all_pri_2 = pd.concat([df_all_pri_2, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('PRI告警记录无该时间段数据')
                print('PRI告警记录无该时间段数据')

            # Abis_1起止时间点
            df_all_abis_1 = pd.DataFrame()
            time_abis_1_x_num = 0
            for i in range(row):
                time_abis_1_x = str(df.loc[i, 'Abis_触发时间_1'])
                if time_abis_1_x == '' or time_abis_1_x == ' ':
                    time_abis_1_x_num = time_abis_1_x_num + 1
                    continue
                elif time_abis_1_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_abis_1_x) == 1:
                        df_list_abis_1 = pd.DataFrame()
                        for j in range(41, 61):
                            df_list_abis_1 = pd.concat([df_list_abis_1, xl.iloc[[i], [j]]], axis=1)
                        df_all_abis_1 = pd.concat([df_all_abis_1, df_list_abis_1], axis=0)
            if df_all_abis_1.empty:
                for j in range(41, 61):
                    df_all_abis_1 = pd.concat([df_all_abis_1, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('Abis信令详情无该时间段数据')
                print('Abis信令详情无该时间段数据')

            # Abis_2起止时间点
            df_all_abis_2 = pd.DataFrame()
            time_abis_2_x_num = 0
            for i in range(row):
                time_abis_2_x = str(df.loc[i, 'Abis_切换发起时间_2'])
                if time_abis_2_x == '' or time_abis_2_x == ' ':
                    time_abis_2_x_num = time_abis_2_x_num + 1
                    continue
                elif time_abis_2_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_abis_2_x) == 1:
                        df_list_abis_2 = pd.DataFrame()
                        for j in range(61, 84):
                            df_list_abis_2 = pd.concat([df_list_abis_2, xl.iloc[[i], [j]]], axis=1)
                        df_all_abis_2 = pd.concat([df_all_abis_2, df_list_abis_2], axis=0)
            if df_all_abis_2.empty:
                for j in range(61, 84):
                    df_all_abis_2 = pd.concat([df_all_abis_2, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('Abis切换详情无该时间段数据')
                print('Abis切换详情无该时间段数据')

            # Abis_3起止时间点
            df_all_abis_3 = pd.DataFrame()
            time_abis_3_x_num = 0
            for i in range(row):
                time_abis_3_x = str(df.loc[i, 'Abis_触发时间_3'])
                if time_abis_3_x == '' or time_abis_3_x == ' ' or time_abis_3_x == '  ':
                    time_abis_3_x_num = time_abis_3_x_num + 1
                    continue
                elif time_abis_3_x_num >= 300:
                    break
                else:
                    try:
                        if self.time_location(time_first_int, time_last_int, time_abis_3_x) == 1:
                            df_list_abis_3 = pd.DataFrame()
                            for j in range(84, 127):
                                df_list_abis_3 = pd.concat([df_list_abis_3, xl.iloc[[i], [j]]], axis=1)
                            df_all_abis_3 = pd.concat([df_all_abis_3, df_list_abis_3], axis=0)
                    except:
                        xxxxx = 1
            if df_all_abis_3.empty:
                for j in range(84, 127):
                    df_all_abis_3 = pd.concat([df_all_abis_3, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('Abis测量报告无该时间段数据')
                print('Abis测量报告无该时间段数据')

            # A_1起止时间点
            df_all_a_1 = pd.DataFrame()
            time_a_1_x_num = 0
            for i in range(row):
                time_a_1_x = str(df.loc[i, 'A_触发时间_1'])
                if time_a_1_x == '' or time_a_1_x == ' ':
                    time_a_1_x_num = time_a_1_x_num + 1
                    continue
                elif time_a_1_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_a_1_x) == 1:
                        df_list_a_1 = pd.DataFrame()
                        for j in range(127, 151):
                            df_list_a_1 = pd.concat([df_list_a_1, xl.iloc[[i], [j]]], axis=1)
                        df_all_a_1 = pd.concat([df_all_a_1, df_list_a_1], axis=0)
            if df_all_a_1.empty:
                for j in range(127, 151):
                    df_all_a_1 = pd.concat([df_all_a_1, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('A信令详情无该时间段数据')
                print('A信令详情无该时间段数据')

            # A_2起止时间点
            df_all_a_2 = pd.DataFrame()
            time_a_2_x_num = 0
            for i in range(row):
                time_a_2_x = str(df.loc[i, 'A_切换结束时间_2'])
                if time_a_2_x == '' or time_a_2_x == ' ':
                    time_a_2_x_num = time_a_2_x_num + 1
                    continue
                elif time_a_2_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_a_2_x) == 1:
                        df_list_a_2 = pd.DataFrame()
                        for j in range(151, 174):
                            df_list_a_2 = pd.concat([df_list_a_2, xl.iloc[[i], [j]]], axis=1)
                        df_all_a_2 = pd.concat([df_all_a_2, df_list_a_2], axis=0)
            if df_all_a_2.empty:
                for j in range(151, 174):
                    df_all_a_2 = pd.concat([df_all_a_2, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('A切换详情无该时间段数据')
                print('A切换详情无该时间段数据')
            df_all_pri.to_excel('.\\data\\excel_time\\intermediate\\df_all_pri.xlsx', sheet_name='Sheet1', index=False, header=True)
            df_all_pri_2.to_excel('.\\data\\excel_time\\intermediate\\df_all_pri_2.xlsx', sheet_name='Sheet1', index=False, header=True)
            df_all_abis_1.to_excel('.\\data\\excel_time\\intermediate\\df_all_abis_1.xlsx', sheet_name='Sheet1', index=False, header=True)
            df_all_abis_2.to_excel('.\\data\\excel_time\\intermediate\\df_all_abis_2.xlsx', sheet_name='Sheet1', index=False, header=True)
            df_all_abis_3.to_excel('.\\data\\excel_time\\intermediate\\df_all_abis_3.xlsx', sheet_name='Sheet1', index=False, header=True)
            df_all_a_1.to_excel('.\\data\\excel_time\\intermediate\\df_all_a_1.xlsx', sheet_name='Sheet1', index=False, header=True)
            df_all_a_2.to_excel('.\\data\\excel_time\\intermediate\\df_all_a_2.xlsx', sheet_name='Sheet1', index=False, header=True)

            xl1 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_pri.xlsx', sheet_name='Sheet1')
            xl2 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_pri_2.xlsx', sheet_name='Sheet1')
            xl3 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_abis_1.xlsx', sheet_name='Sheet1')
            xl4 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_abis_2.xlsx', sheet_name='Sheet1')
            xl5 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_abis_3.xlsx', sheet_name='Sheet1')
            xl6 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_a_1.xlsx', sheet_name='Sheet1')
            xl7 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_a_2.xlsx', sheet_name='Sheet1')

            df_all_tonghao = pd.concat([xl1, xl2, xl3, xl4, xl5, xl6, xl7], axis=1)
            df_all_tonghao.to_excel(file_addr_new, sheet_name='Sheet1', index=False, header=True)

        if my_model == '铁科':
            # PRI起止时间点
            df_all_pri = pd.DataFrame()
            time_pri_x_num = 0
            for i in range(row):
                time_pri_x = str(df.loc[i, 'PRI_触发时间_1'])
                if time_pri_x == '' or time_pri_x == ' ':
                    time_pri_x_num = time_pri_x_num + 1
                    continue
                elif time_pri_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_pri_x) == 1:
                        df_list_pri = pd.DataFrame()
                        for j in range(0, 23):
                            df_list_pri = pd.concat([df_list_pri, xl.iloc[[i], [j]]], axis=1)
                        df_all_pri = pd.concat([df_all_pri, df_list_pri], axis=0)
            if df_all_pri.empty:
                for j in range(0, 23):
                    df_all_pri = pd.concat([df_all_pri, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('PRI接口信令无该时间段数据')
                print('PRI接口信令无该时间段数据')

            # Abis_1起止时间点
            df_all_abis_1 = pd.DataFrame()
            time_abis_1_x_num = 0
            for i in range(row):
                time_abis_1_x = str(df.loc[i, 'Abis_触发时间_1'])
                if time_abis_1_x == '' or time_abis_1_x == ' ':
                    time_abis_1_x_num = time_abis_1_x_num + 1
                    continue
                elif time_abis_1_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_abis_1_x) == 1:
                        df_list_abis_1 = pd.DataFrame()
                        for j in range(23, 41):
                            df_list_abis_1 = pd.concat([df_list_abis_1, xl.iloc[[i], [j]]], axis=1)
                        df_all_abis_1 = pd.concat([df_all_abis_1, df_list_abis_1], axis=0)
            if df_all_abis_1.empty:
                for j in range(23, 41):
                    df_all_abis_1 = pd.concat([df_all_abis_1, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('Abis信令无该时间段数据')
                print('Abis信令无该时间段数据')

            # Abis_2起止时间点
            df_all_abis_2 = pd.DataFrame()
            time_abis_2_x_num = 0
            for i in range(row):
                time_abis_2_x = str(df.loc[i, 'Abis_触发时间_2'])
                if time_abis_2_x == '' or time_abis_2_x == ' ':
                    time_abis_2_x_num = time_abis_2_x_num + 1
                    continue
                elif time_abis_2_x_num >= 50:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_abis_2_x) == 1:
                        df_list_abis_2 = pd.DataFrame()
                        for j in range(41, 78):
                            df_list_abis_2 = pd.concat([df_list_abis_2, xl.iloc[[i], [j]]], axis=1)
                        df_all_abis_2 = pd.concat([df_all_abis_2, df_list_abis_2], axis=0)
            if df_all_abis_2.empty:
                for j in range(41, 78):
                    df_all_abis_2 = pd.concat([df_all_abis_2, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('Abis测量报告无该时间段数据')
                print('Abis测量报告无该时间段数据')

            # Abis_3起止时间点
            df_all_abis_3 = pd.DataFrame()
            time_abis_3_x_num = 0
            for i in range(row):
                time_abis_3_x = str(df.loc[i, 'Abis_切换发起时间_3'])
                if time_abis_3_x == '' or time_abis_3_x == ' ' or time_abis_3_x == '  ':
                    time_abis_3_x_num = time_abis_3_x_num + 1
                    continue
                elif time_abis_3_x_num >= 5:
                    break
                else:
                    try:
                        if self.time_location(time_first_int, time_last_int, time_abis_3_x) == 1:
                            df_list_abis_3 = pd.DataFrame()
                            for j in range(78, 95):
                                df_list_abis_3 = pd.concat([df_list_abis_3, xl.iloc[[i], [j]]], axis=1)
                            df_all_abis_3 = pd.concat([df_all_abis_3, df_list_abis_3], axis=0)
                    except:
                        xxxxx = 1
            if df_all_abis_3.empty:
                for j in range(78, 95):
                    df_all_abis_3 = pd.concat([df_all_abis_3, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('Abis切换记录无该时间段数据')
                print('Abis切换记录无该时间段数据')

            # A_1起止时间点
            df_all_a_1 = pd.DataFrame()
            time_a_1_x_num = 0
            for i in range(row):
                time_a_1_x = str(df.loc[i, 'A_触发时间_1'])
                if time_a_1_x == '' or time_a_1_x == ' ':
                    time_a_1_x_num = time_a_1_x_num + 1
                    continue
                elif time_a_1_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_a_1_x) == 1:
                        df_list_a_1 = pd.DataFrame()
                        for j in range(95, 110):
                            df_list_a_1 = pd.concat([df_list_a_1, xl.iloc[[i], [j]]], axis=1)
                        df_all_a_1 = pd.concat([df_all_a_1, df_list_a_1], axis=0)
            if df_all_a_1.empty:
                for j in range(95, 110):
                    df_all_a_1 = pd.concat([df_all_a_1, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('A接口信令无该时间段数据')
                print('A接口信令无该时间段数据')

            # A_2起止时间点
            df_all_a_2 = pd.DataFrame()
            time_a_2_x_num = 0
            for i in range(row):
                time_a_2_x = str(df.loc[i, 'A_切换结束时间_2'])
                if time_a_2_x == '' or time_a_2_x == ' ':
                    time_a_2_x_num = time_a_2_x_num + 1
                    continue
                elif time_a_2_x_num >= 5:
                    break
                else:
                    if self.time_location(time_first_int, time_last_int, time_a_2_x) == 1:
                        df_list_a_2 = pd.DataFrame()
                        for j in range(110, 123):
                            df_list_a_2 = pd.concat([df_list_a_2, xl.iloc[[i], [j]]], axis=1)
                        df_all_a_2 = pd.concat([df_all_a_2, df_list_a_2], axis=0)
            if df_all_a_2.empty:
                for j in range(110, 123):
                    df_all_a_2 = pd.concat([df_all_a_2, xl.iloc[[-1], [j]]], axis=1)
                empty_exl.append('A切换记录无该时间段数据')
                print('A切换记录无该时间段数据')

            df_all_pri.to_excel('.\\data\\excel_time\\intermediate\\df_all_pri.xls', sheet_name='Sheet1', index=False, header=True)
            df_all_abis_1.to_excel('.\\data\\excel_time\\intermediate\\df_all_abis_1.xls', sheet_name='Sheet1', index=False, header=True)
            df_all_abis_2.to_excel('.\\data\\excel_time\\intermediate\\df_all_abis_2.xls', sheet_name='Sheet1', index=False, header=True)
            df_all_abis_3.to_excel('.\\data\\excel_time\\intermediate\\df_all_abis_3.xls', sheet_name='Sheet1', index=False, header=True)
            df_all_a_1.to_excel('.\\data\\excel_time\\intermediate\\df_all_a_1.xls', sheet_name='Sheet1', index=False, header=True)
            df_all_a_2.to_excel('.\\data\\excel_time\\intermediate\\df_all_a_2.xls', sheet_name='Sheet1', index=False, header=True)

            xl1 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_pri.xls', sheet_name='Sheet1')
            xl3 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_abis_1.xls', sheet_name='Sheet1')
            xl4 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_abis_2.xls', sheet_name='Sheet1')
            xl5 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_abis_3.xls', sheet_name='Sheet1')
            xl6 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_a_1.xls', sheet_name='Sheet1')
            xl7 = pd.read_excel('.\\data\\excel_time\\intermediate\\df_all_a_2.xls', sheet_name='Sheet1')

            df_all_tieke = pd.concat([xl1, xl3, xl4, xl5, xl6, xl7], axis=1)
            df_all_tieke.to_excel(file_addr_new, sheet_name='Sheet1', index=False, header=True)
        return err_ip, empty_exl