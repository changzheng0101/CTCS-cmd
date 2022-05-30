import re
import pandas as pd
from dateutil.parser import parse
# time_1 切换发起时间 time_2 切换结束时间 time_3 故障触发时间
# 判断故障发生时，是否正在发生切换
def time_xqqh(time_1, time_2, time_3):
    # time_1_aa = time_1.split(':')[0]
    # time_1_a = int(time_1_aa.split(' ')[1])  # 时
    # time_1_b = int(time_1.split(':')[1])    # 分
    # time_1_cc = time_1.split(':')[2]
    # time_1_c = int(re.split('[^0-9]+', time_1_cc)[0])  # 秒
    # time_1_d = int(re.split('[^0-9]+', time_1_cc)[-1])
    #
    # time_2_aa = time_2.split(':')[0]
    # time_2_a = int(time_2_aa.split(' ')[1])
    # time_2_b = int(time_2.split(':')[1])
    # time_2_cc = time_2.split(':')[2]
    # time_2_c = int(re.split('[^0-9]+', time_2_cc)[0])
    # time_2_d = int(re.split('[^0-9]+', time_2_cc)[-1])
    #
    # time_3_aa = time_3.split(':')[0]
    # time_3_a = int(time_3_aa.split(' ')[1])
    # time_3_b = int(time_3.split(':')[1])
    # time_3_cc = time_3.split(':')[2]
    # time_3_c = int(re.split('[^0-9]+', time_3_cc)[0])
    # time_3_d = int(re.split('[^0-9]+', time_3_cc)[-1])
    #
    # if time_1_a <= time_3_a and time_1_b <= time_3_b and time_1_c <= time_3_c:
    #     if time_3_a <= time_2_a and time_3_b <= time_2_b and time_3_c <= time_2_c:
    #         # 小区切换时and time_3_c <= time_2_c
    #         return 1
    #     elif time_3_a == time_2_a and time_3_b == time_2_b and time_3_c - time_2_c <= 5:
    #         # 小区切换后导致上下行变化
    #         return 2
    #     elif time_3_a == time_2_a and time_3_b - time_2_b <= 1 and time_2_c - time_3_c <= 5:
    #         # 小区切换后导致上下行变化
    #         return 2
    # else:
    #     # 与小区切换无关
    #     return 0
    if (parse(time_3)-parse(time_1)).days >= 0:
        if (parse(time_2)-parse(time_3)).days >= 0:
            return 1
        if (parse(time_2)-parse(time_3)).seconds <= 5:
            return 2
    else:
        return 0

# 时间time_1与time_2比大小，小的先发生, 返回值0表示time_1大，返回值1表示time_2大
def time_comparision(time_1, time_2):
    time_1_aa = time_1.split(':')[0]
    time_1_a = int(time_1_aa.split(' ')[1]) # 时
    time_1_b = int(time_1.split(':')[1]) # 分
    time_1_cc = time_1.split(':')[2]
    time_1_c = int(time_1_cc.split('.')[0]) # 秒
    time_1_d = int(time_1_cc.split('.')[1]) # 厘

    time_2_aa = time_2.split(':')[0]
    time_2_a = int(time_2_aa.split(' ')[1])
    time_2_b = int(time_2.split(':')[1])
    time_2_cc = time_2.split(':')[2]
    time_2_c = int(time_2_cc.split('.')[0])
    time_2_d = int(time_2_cc.split('.')[1])

    if time_1_a < time_2_a:
        return 1
    elif time_1_a > time_2_a:
        return 0
    else:
        if time_1_b < time_2_b:
            return 1
        elif time_1_b > time_2_b:
            return 0
        else:
            if time_1_c < time_2_c:
                return 1
            elif time_1_c > time_2_c:
                return 0
            else:
                if time_1_d < time_2_d:
                    return 1
                elif time_1_d > time_2_d:
                    return 0

# 输入两个时间，每个元素相比，相差x秒认为相关。time_1先发生，time_2后发生，相关输出1，否则输出0
def time_xiangguan(time_1, time_2 ,x):
    x = int(x)
    time_1_list = re.split('[^0-9]+', time_1)
    time_1_nian = int(time_1_list[0])  # 年
    time_1_yue = int(time_1_list[1])  # 月
    time_1_ri = int(time_1_list[2])  # 日
    time_1_shi = int(time_1_list[3])  # 时
    time_1_fen = int(time_1_list[4])  # 分
    time_1_miao = int(time_1_list[5])  # 秒
    time_1_li = int(time_1_list[6])  # 厘

    time_2_list = re.split('[^0-9]+', time_2)
    time_2_nian = int(time_2_list[0])  # 年
    time_2_yue = int(time_2_list[1])  # 月
    time_2_ri = int(time_2_list[2])  # 日
    time_2_shi = int(time_2_list[3])  # 时
    time_2_fen = int(time_2_list[4])  # 分
    time_2_miao = int(time_2_list[5])  # 秒
    time_2_li = int(time_2_list[6])  # 厘

    if time_1_nian == time_2_nian:
        if time_1_yue == time_2_yue:
            if time_1_ri == time_2_ri:
                if time_1_shi == time_2_shi:
                    if time_1_fen == time_2_fen:
                        if time_2_miao - time_1_miao <= x:
                            return 1  # 差x秒
                        else:
                            return 0
                    elif time_2_fen - time_1_fen == 1:
                        if time_1_miao - time_2_miao >= 60-x :
                            return 1  # 差x秒，但是分已进一位
                        else:
                            return 0
                elif time_2_shi - time_1_shi == 1:
                    if time_1_fen - time_2_fen == 59:
                        if time_1_miao - time_2_miao >= 60-x:
                            return 1  # 差x秒，时进一位
                        else:
                            return 0
                    else:
                        return 0
            elif time_2_ri - time_1_ri  == 1:
                if time_1_shi - time_2_shi == 23:
                    if time_1_fen - time_2_fen == 59:
                        if time_1_miao - time_2_miao >= 60-x:
                            return 1
                        else:
                            return 0
                    else:
                        return 0
                else:
                    return 0
        if time_2_yue - time_1_yue == 1:
            if time_1_yue == 1 or time_1_yue == 3 or time_1_yue == 5 or time_1_yue == 7 or time_1_yue == 8 or time_1_yue == 10 or time_1_yue == 12:
                if time_1_ri - time_2_ri  == 31:
                    if time_1_shi - time_2_shi == 23:
                        if time_1_fen - time_2_fen == 59:
                            if time_1_miao - time_2_miao >= 60-x:
                                return 1
                            else:
                                return 0
                        else:
                            return 0
                    else:
                        return 0
                else:
                    return 0
            elif time_1_yue == 4 or time_1_yue == 6 or time_1_yue == 9 or time_1_yue == 11:
                if time_1_ri - time_2_ri  == 30:
                    if time_1_shi - time_2_shi == 23:
                        if time_1_fen - time_2_fen == 59:
                            if time_1_miao - time_2_miao >= 60-x:
                                return 1
                            else:
                                return 0
                        else:
                            return 0
                    else:
                        return 0
                else:
                    return 0
            elif time_1_yue == 2:
                if time_1_nian % 4 == 0:
                    if time_1_ri - time_2_ri == 29:
                        if time_1_shi - time_2_shi == 23:
                            if time_1_fen - time_2_fen == 59:
                                if time_1_miao - time_2_miao >= 60 - x:
                                    return 1
                                else:
                                    return 0
                            else:
                                return 0
                        else:
                            return 0
                    else:
                        return 0
                if time_1_nian % 4 != 0:
                    if time_1_ri - time_2_ri == 28:
                        if time_1_shi - time_2_shi == 23:
                            if time_1_fen - time_2_fen == 59:
                                if time_1_miao - time_2_miao >= 60 - x:
                                    return 1
                                else:
                                    return 0
                            else:
                                return 0
                        else:
                            return 0
                    else:
                        return 0
    elif time_2_nian - time_1_nian == 1:
        if time_1_yue - time_2_yue == 11:
            if time_1_ri - time_2_ri == 30:
                if time_1_shi - time_2_shi == 23:
                    if time_1_fen - time_2_fen == 59:
                        if time_1_miao - time_2_miao >= 60-x:
                            return 1
                        else:
                            return 0
                    else:
                        return 0
                else:
                    return 0
            else:
                return 0
        else:
            return 0
    else:
        return 0

def time_location(time_find, x): # x是挨个遍历的数，time_find对比
    x_1 = re.split('[^0-9]+', x)[:-1]
    x_1_int = int(x_1[3]) * (10 ** 4) + int(x_1[4]) * (10 ** 2) + int(x_1[5])
    if time_find - x_1_int == 1:    # 找到终止点，time_last_int相等的时间点刚过的第一个时间点；
        return 1
    else:
        return 0

# 上数K秒，返回行数
def time_k_gap(dir_path, time_row ,k): # time_1是输入时间(disconnent时间)，time_row是其所在行
    df = pd.read_excel(dir_path)
    time_1 = str(df.loc[time_row, '触发时间'])
    time_1_list = re.split('[^0-9]+', time_1)
    time_1_shi = int(time_1_list[3])  # 时
    time_1_fen = int(time_1_list[4])  # 分
    time_1_miao = int(time_1_list[5])  # 秒
    if time_1_miao >= k:
        gap = time_1_miao-k
        out_p = int(time_1_list[3]) * (10 ** 4) + int(time_1_list[4]) * (10 ** 2) + gap
    elif time_1_miao < k:
        gap = time_1_miao + 60 - k
        if time_1_fen != 0:
            out_p = int(time_1_list[3]) * (10 ** 4) + (int(time_1_list[4])-1) * (10 ** 2) + gap
        else:
            out_p = (int(time_1_list[3])-1) * (10 ** 4) + 59 * (10 ** 2) + gap

    for i in range(time_row, 1, -1):
        pri_time_1 = str(df.loc[i, '触发时间'])
        if time_location(out_p, pri_time_1) == 1:
            out_put_row = time_row - i
            return out_put_row
'''
dir_path='F:\CTCS3\\202-10-18测试数据\T1定时器\\3097-01-上海-2021-05-28-京沪-G159-T1定时器问题\\3097-01-上海-21-05-28-京沪-G159-\G159\PRI口详细信息_(G159)(400BF-3097-01)(14984174941)(2021-05-28 211233.110—2021-05-28 213408.164).xls'
time_1 = '2021-05-28 21:34:07.867'
time_row = 7015
k= 6
print(time_k_gap(dir_path, time_row ,k))
'''




