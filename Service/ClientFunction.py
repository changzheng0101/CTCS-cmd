# -*- coding: utf-8 -*-
"""
@File    :  ClientFunction.py
@Time    :  2022/5/3 09:54
@Author  :  zhuyugang
@Version :  1.0
@Desc    :  完成客户端基础处理
"""
from Service.NetworkClientService import *

def analysisbtn(IP, cc, FTP_username, FTP_password,train_num,transID,var_filename, time_first, time_last):
    if '\\' in var_filename:
        file_addr = var_filename
    else:
        file_addr = '.\\data'
    filename = train_num + '-' + transID + '-Data'
    if time_first == '': # 判断输入的截取时间是否为空
        time_first = '0'
    if time_last == '':
        time_last = '0'
    elif time_first == '' and len(time_last.split('-')) == 6:
        time_type = 2
    elif len(time_first.split('-')) == 6 and time_last == '':
        time_type = 3
    elif len(time_first.split('-')) == 6 and len(time_last.split('-')) == 6:
        time_type = 4
    else:
        print('时间输入格式错误！')
        return 0
    if train_num == '':
        print('故障数据文件名输入错误！')
        return 0
    if transID == '':
        print('故障数据文件名输入错误！')
        return 0
    else:
        pass

    if train_num == '' or transID == '' or filename == '':
        print('未输入相关信息！')
        return 0
    else:
        connServer(IP, cc, FTP_username, FTP_password, train_num, transID, filename, time_first, time_last, file_addr)
