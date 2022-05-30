# -*- coding: utf-8 -*-
"""
@File    :  NetworkServerService.py
@Time    :  2022/3/31 19:40
@Author  :  changzheng
@Version :  1.0
@Desc    :  负责服务器的网络相关的工作
"""
import multiprocessing
import os
import shutil
import socket
import time
from multiprocessing import Process, Queue
import matplotlib.pyplot as plt
from CTCSutils.DOM import *
from Service.DataProcess.DataProcess import analyRun
from Service.ServerFunction import *
import datetime
users = {}
global ss, r1, stop_threading, exitcode

plt.switch_backend('agg')

class NetworkServerService:
    def __init__(self):
        pass

def startServer(IP, PORT, mysql_username, mysql_passowrd, ftp_data, ftp_output):
    global ss, stop_threading, exitcode
    ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        ss.bind((IP, int(PORT)))
        ss.listen(30)
        print("*****************************\n"
              "      故障分析系统已启动...\n"
              "*****************************\n")
        # print("CTCS>")
    except Exception:
        print("*****************************\n"
              "     IP地址或端口号错误！！！\n"
              "*****************************\n")
        # print("CTCS>")
        return 1

    data_path = ftp_data
    result_file_dirs = os.listdir(data_path)
    if len(result_file_dirs) == 30:
        filename_remove = data_path + '\\' + result_file_dirs[0]
        shutil.rmtree(filename_remove)
    date01 = datetime.date.today()
    data_path = data_path + "\\" + str(date01.year) + "-" + str(date01.month) + "-" + str(date01.day)
    if not os.path.exists(data_path):
        os.mkdir(data_path)
    while True:
        print('服务器已启动...')
        try:
            cc, addr = ss.accept()
        except Exception:
            break
        request_msg = cc.recv(100000).decode('utf-8')
        if len(users) == 30:
            dom = DOM()
            dom.dom_readXML(request_msg)
            request_ack_msg = dom.dom_writeXML('2', '1')
            cc.send(request_ack_msg.encode('utf-8'))
            cc.close()
        elif request_msg == 'close':
            cc.send('OK'.encode('utf-8'))
            break
        elif request_msg == 'test':
            cc.close()
        else:

            st = Process(target=msg_chat, args=(cc, mysql_username, mysql_passowrd, ftp_output, data_path, request_msg))
            st.start()
    ss.close()

def msg_chat(cc, mysql_username, mysql_password, ftp_output, data_path, msg):
    global trans_ID, flag, finish_ack_msg, request_msg
    flag = 1

    try:
        while flag:
            request_msg = msg
            if not request_msg:
                request_msg = cc.recv(100000).decode('utf-8')
                continue
            if "xml" in request_msg:
                dom = DOM()
                dom.dom_readXML(request_msg)
                if dom.getContentFlag() == '1':
                    analyzeData(cc, mysql_username, mysql_password, ftp_output, data_path)
                    break
            elif 'query_by_id' in request_msg:
                msg_lists = request_msg.split()
                train_num = msg_lists[1]
                root_in = msg_lists[2]
                pw = msg_lists[3]
                record_result = btn_click(train_num, root_in, pw)
                cc.send(record_result.encode('utf-8'))
                cc.close()
                break
            elif 'accuracy' in request_msg:
                accuracy_str = AI_Classify()
                cc.send(accuracy_str.encode('utf-8'))
                cc.close()
                break
            else:
                break

    except Exception as e:
        print(e)
        print(trans_ID + '断开连接')
        cc.close()

def analyzeData(cc, mysql_username, mysql_password, ftp_output, data_path):
    # flag = 1
    global trans_ID, flag, finish_ack_msg
    try:
        dom = DOM()
        dom.dom_readXML(request_msg)
        train_num = dom.getTrainNum()
        trans_ID = dom.getTransID()
        # my_model = dom.get_data_model()
        time_first_1 = dom.getTimeFirst()
        time_last_1 = dom.getTimeLast()
        time_first = time_first_1.rstrip('\n')
        time_last = time_last_1.rstrip('\n')
        if time_first == '0':
            time_first = ''
        if time_last == '0':
            time_last = ''
        filename = train_num + '-' + trans_ID + '-Data'
        users[trans_ID] = cc
        # else:
        #     pass
        # 回复确认通知消息
        request_ack_msg = dom.dom_writeXML('2', '0')  # 生成回复确认消息XML报文

        for k, v in users.items():
            if k == trans_ID:
                v.settimeout(600)
                v.send(request_ack_msg.encode('utf-8'))
                # msg = cc.recv(100000).decode('utf-8')
                '''
                    智能分析数据文件，并生成故障结果
                '''
                # my_model, filename, time_first, time_last = msg.split()

                print('数据信息已接受...')

                # 创建文件夹和删除文件夹
                # result_path = "E:\\tonghaoyuan\\output"
                # result_path = var_FTP_output.get()
                result_path = ftp_output
                result_file_dirs = os.listdir(result_path)
                if len(result_file_dirs) == 30:
                    filename_remove = result_path + '\\' + result_file_dirs[0]
                    shutil.rmtree(filename_remove)
                date01 = datetime.date.today()
                result_path = result_path + "\\" + str(date01.year) + "-" + str(date01.month) + "-" + str(
                    date01.day)
                if not os.path.exists(result_path):
                    os.mkdir(result_path)

                # # 从FTP系统中下载故障数据文件
                # ftp = ftpconnect('222.199.220.243', 'ftpTest', '123456')
                # var_local_A1 = 'E:\\tonghaoyuan\\data'
                # var_local_A1 = var_FTP_data.get()
                var_local_A1 = data_path
                data_localpath = var_local_A1 + '\\' + filename + '.rar'
                file_addr2 = data_localpath
                # data_remotepath = '.\\data\\' + filename + '.rar'
                # output_remotepath = '.\\output\\' + filename + '_output.txt'
                # downloadfile(ftp, data_remotepath, data_localpath)
                # print('文件下载中...')
                # # v.send('正在处理数据....'.encode('utf-8'))
                # # 处理数据
                pri_row_num = ''
                abis_row_num = ''
                PRIdes = ''
                Abisdes = ''
                x1 = result_path
                time_direc_tab, PRI_rowNum, Abis_rowNum, A_TZ, A_rowNum, XX, v = analyRun(mysql_username,
                                                                                          mysql_password,
                                                                                          time_first,
                                                                                          time_last, file_addr2,
                                                                                          x1,
                                                                                          v, train_num,
                                                                                          trans_ID)

                # 生成结果文件
                A_infodes = []
                Abis_infodes = []
                PRI_infodes = []
                if time_direc_tab != []:
                    Happentime = time_direc_tab[0].split('：', 1)[1].split('；', 1)[0].rsplit('.', 1)[0]
                    ExcepType = time_direc_tab[0].split('，', 1)[0]
                    if '故障类型' in ExcepType:
                        pass
                    else:
                        ExcepType = '不明原因'

                    for i in range(len(time_direc_tab)):
                        if 'A接口' in time_direc_tab[i]:
                            A_infodes.append(time_direc_tab[i])
                        if 'Abis接口' in time_direc_tab[i]:
                            Abis_infodes.append(time_direc_tab[i])
                        if 'PRI接口' in time_direc_tab[i]:
                            PRI_infodes.append(time_direc_tab[i])
                    if Abis_infodes != []:
                        Abisdes = str(Abis_infodes[0])
                        for i in range(1, len(Abis_infodes)):
                            Abisdes = Abisdes + '.' + str(Abis_infodes[i])
                    else:
                        Abisdes = ''

                    if A_infodes != []:
                        Ades = str(A_infodes[0])
                        for i in range(1, len(A_infodes)):
                            Ades = Ades + '.' + str(A_infodes[i])
                    else:
                        Ades = ''

                    if PRI_infodes != []:
                        PRIdes = str(PRI_infodes[0])
                        for i in range(1, len(PRI_infodes)):
                            PRIdes = PRIdes + '.' + str(PRI_infodes[i])
                    else:
                        PRIdes = ''
                else:
                    Happentime = '未找到相关故障时间'
                    ExcepType = '不明原因'
                    Ades = ''

                if PRI_rowNum != []:
                    pri_row_num = str(PRI_rowNum[0])
                    for i in range(1, len(PRI_rowNum)):
                        pri_row_num = pri_row_num + '&' + str(PRI_rowNum[i])
                else:
                    pri_row_num = ''

                if Abis_rowNum != []:
                    abis_row_num = str(Abis_rowNum[0])
                    for i in range(1, len(Abis_rowNum)):
                        abis_row_num = abis_row_num + '&' + str(Abis_rowNum[i])
                else:
                    abis_row_num = ''

                if A_rowNum != []:
                    a_row_num = str(A_rowNum[0])
                    for i in range(1, len(A_rowNum)):
                        a_row_num = a_row_num + '&' + str(A_rowNum[i])
                else:
                    a_row_num = ''

                AnalyseDes = str(A_TZ[0])
                for i in range(1, len(A_TZ)):
                    AnalyseDes = AnalyseDes + ',' + str(A_TZ[i])

                result_path_xml = result_path + '\\' + '故障分析结果_' + train_num + '_' + trans_ID + '.xml'
                dom.write_output_xml(result_path_xml, train_num, Happentime, trans_ID, ExcepType, abis_row_num,
                                 Abisdes,
                                 a_row_num, Ades, pri_row_num, PRIdes, AnalyseDes, XX[0])

                # 发送故障分析完成通知
                time.sleep(0.5)
                finish_msg = dom.dom_writeXML('3', '0')
                v.send(finish_msg.encode('utf-8'))
                for i in range(3):
                    try:
                        cc.settimeout(10)
                        finish_ack_msg = cc.recv(100000).decode('utf-8')
                        break
                    except Exception:
                        time.sleep(0.5)
                        v.send(finish_msg.encode('utf-8'))
                        # print(i)
                cc.settimeout(None)
                # print(finish_ack_msg)
                dom.dom_readXML(finish_ack_msg)
                if dom.getContentFlag() == '4':
                    v.send('analyze_exit'.encode('utf-8'))
                    print(trans_ID + '故障分析完成...')
                    del users[trans_ID]
                    v.close()
                    flag = 0
                    return
    except Exception as e:
        print(e)
        print(trans_ID + '断开连接')
        for k, v in users.items():
            if k == trans_ID:
                v.send('已断开连接...'.encode('utf-8'))
                v.close()
        del users[trans_ID]
        return