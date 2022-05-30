# -*- coding: utf-8 -*-
"""
@File    :  BaseJudgeService.py
@Time    :  2022/3/31 19:28
@Author  :  changzheng
@Version :  1.0
@Desc    :  对于三种类型都适用的故障判断方法
"""
import os

import pymysql


class BaseJudgeService(object):
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def connectsql(self, str):
        # 连接数据库
        try:
            self.connection = pymysql.connect(host='localhost',  # 数据库地址
                                              user=self.username,  # 数据库用户名
                                              password=self.password,  # 数据库密码
                                              db=str,  # 数据库名称
                                              charset='utf8mb4',
                                              cursorclass=pymysql.cursors.DictCursor)
        except pymysql.err.OperationalError:
            print("ERROR:用户名或密码错误！\n")
            input("输入任意键退出！")
            exit(1)

    def connectexit(self):
        self.connection.cursor().close()
        self.connection.close()

    # 搜索log接口数据txt（安全层mac和SaPDU长度错误）
    def log_search(self, log_path):
        mac = 0
        for home, dirs, files in os.walk(log_path):  # 找到子路径里所有文件
            for file in files:
                if os.path.splitext(file)[1] == '.txt':  # 在所有文件中找到后缀为txt的文件
                    txt_1 = os.path.splitext(file)[0] + '.txt'
                    txt_S = home + '//' + txt_1
                    file_data = ""
                    with open(txt_S, 'r', encoding='gb18030') as f:  # 解决编码问题
                        for line in f:
                            line = line.replace(' ', '')
                            file_data += line
                    with open(txt_S, "w", encoding='gb18030') as f:
                        f.write(file_data)
                    with open(txt_S, 'r', encoding='gb18030') as fr:
                        if 'MACerror' in fr.read():
                            mac = 1
                            break
                        elif 'SaPDUlengtherror' in fr.read():
                            mac = 2
                            break
                        elif 'regStat=3' in fr.read():
                            mac = 3
                            break
        if mac == 1:
            Update_db = '安全层MAC'
        elif mac == 2:
            Update_db = 'SaPDU长度错误'
        elif mac == 3:
            Update_db = 'SIM卡脱网'
        else:
            Update_db = '不明原因'
        return (Update_db)

    # 获得列名所对应的列号
    def getColumnIndex(self, table, columnName):
        columnIndex = None
        for i in range(table.ncols):
            if table.cell_value(0, i) == columnName:
                columnIndex = i
            else:
                pass
        return columnIndex

    # 深度搜索FRMR
    def frmr_deepsearch(self, frmr_value, mode):
        icount = 0
        Update_db = '不明原因'
        if mode == '通号':
            for str in frmr_value:
                if str == '7e':
                    # 查找信息字段的最后一个字段，判别类型
                    msg_tmp = bin(int(frmr_value[icount + 7], 16))

                    if msg_tmp[-2:] == '11':
                        Update_db = 'frmr类型wx'
                    elif msg_tmp[-3] == '1':
                        Update_db = 'frmr类型y'
                    else:
                        Update_db = 'frmr类型z'
                    break
                else:
                    icount = icount + 1
        elif mode == '铁科':
            pass
        else:
            # 查找信息字段的最后一个字段，判别类型
            if len(frmr_value) <= 6:
                pass
            else:
                msg_tmp = bin(int(frmr_value[icount + 6], 16))
                if msg_tmp[-2:] == '11':
                    Update_db = 'frmr类型wx'
                elif msg_tmp[-3] == '1':
                    Update_db = 'frmr类型y'
                else:
                    Update_db = 'frmr类型z'

        return Update_db
