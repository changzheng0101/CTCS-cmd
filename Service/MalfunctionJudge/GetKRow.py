import re
import pandas as pd


def time_location(time_row_data, time_find, x):  # x是挨个遍历的数，time_find对比
    x_1 = re.split('[^0-9]+', x)[:-1]
    x_1_int = int(x_1[3]) * (10 ** 4) + int(x_1[4]) * (10 ** 2) + int(x_1[5])
    if time_find-x_1_int > 1 and time_row_data > x_1_int:  # 找到终止点，time_last_int相等的时间点刚过的第一个时间点；
        return 1
    elif time_row_data < x_1_int:
        return 1
    else:
        return 0


# 上数K秒，返回行数
def time_k_gap(dir_path, time_row, k, mode):  # time_1是输入时间(disconnent时间)，time_row是其所在行
    global out_p, out_put_row
    df = pd.read_excel(dir_path)
    if mode =='通号':
        time_1 = str(df.loc[time_row, 'PRI_触发时间_1'])
    else:
        time_1 = str(df.loc[time_row, 'PRI_触发时间_1'])
    # if mode =='通号':
    #     time_1 = str(df.loc[time_row, '触发时间'])
    # else:
    #     time_1 = str(df.loc[time_row, '触发时间'])
    time_1_list = re.split('[^0-9]+', time_1)
    time_1_shi = int(time_1_list[3])  # 时
    time_1_fen = int(time_1_list[4])  # 分
    time_1_miao = int(time_1_list[5])  # 秒
    time_1_Decimal = time_1_shi * (10 ** 4) + time_1_fen * (10 ** 2) + time_1_miao
    if time_1_miao >= k:
        gap = time_1_miao - k
        out_p = int(time_1_list[3]) * (10 ** 4) + int(time_1_list[4]) * (10 ** 2) + gap
    elif time_1_miao < k:
        gap = time_1_miao + 60 - k
        if time_1_fen != 0:
            out_p = int(time_1_list[3]) * (10 ** 4) + (int(time_1_list[4]) - 1) * (10 ** 2) + gap
        else:
            out_p = (int(time_1_list[3]) - 1) * (10 ** 4) + 59 * (10 ** 2) + gap

    for i in range(time_row, 1, -1):
        pri_time_1 = str(df.loc[i, 'PRI_触发时间_1'])
        # pri_time_1 = str(df.loc[i, '触发时间'])
        if time_location(time_1_Decimal, out_p, pri_time_1 ) == 1:
            out_put_row = time_row - i
            break
    return out_put_row

    # return 10
# if __name__ == "__main__":
#     dir_path = 'D:\\code\\data\\T1测试\\交大-3585-00-合肥-2021-08-19-G8329\\交大-3585-00-合肥-2021-08-19-G8329\\G8329故障\\PRI口详细信息_(G8329)(380B3585-0)(14984173876)(2021-08-19 182634.969—2021-08-19 185756.947).xls'
#     time_row = 12159
#     k = 6
#     mode = '交大'
#     out_put_row = time_k_gap(dir_path, time_row, k, mode)
#     print(out_put_row)