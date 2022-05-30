# -*- coding: utf-8 -*-
"""
@File    :  NetworkClientService.py
@Time    :  2022/3/30 14:19
@Author  :  changzheng
@Version :  1.0
@Desc    :  完成客户端网络的各种连接、处理
"""

import socket
import tkinter as tk
from tkinter import messagebox, NORMAL, DISABLED
from Service.FTPService import FTPService
from CTCSutils.DOM import DOM
import datetime

class NetworkClientService:
    def __init__(self, ip=None, port=0, FTPUserName="", FTPPassword=""):
        self.ClientSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ip = ip
        self.port = int(port)
        self.FTPUserName = FTPUserName
        self.FTPPassword = FTPPassword

    def connect(self):
        self.ClientSock.connect((self.ip, self.port))

    def startCommunication(self, filePath):
        self.sendMessage(filePath)
        dataFromServer = self.ClientSock.recv(1024)
        print(dataFromServer)

    def endCommunication(self):
        self.sendMessage("通信结束")

    def sendMessage(self, message):
        self.ClientSock.send(message.encode('utf-8'))
        print("send" + message)

    def receiveMessage(self):
        data = self.ClientSock.recv(1024)
        print("receive " + data)



    def wm_attributes(self, param, param1):
        pass

def connServer(IP, socket, FTP_username, FTP_password, train_num, transID, filename, time_first, time_last,file_addr):
    global ck, cc
    try:
        cc = socket
        date01 = datetime.date.today()
        result_path = str(date01.year) + "-" + str(date01.month) + "-" + str(date01.day)
        # filename = var_filename.get()
        ftp = FTPService().ftpConnect(IP, FTP_username, FTP_password)
        data_remotepath = '.\\data\\' + result_path + '\\' + filename + '.rar'
        # data_localpath = '.\\data\\' + filename +'.rar'
        if file_addr == '.\\data':
            data_localpath = file_addr + '\\' + filename + '.rar'
        else:
            data_localpath = file_addr
        FTPService().uploadFile(ftp, data_remotepath, data_localpath)

        # 生成请求信息的XML文件
        # train_num = var_train_num.get()
        # transID = var_transID.get()
        dom = DOM(train_num, '', time_first, time_last, '1', transID, '0')
        analysis_request_msg = dom.dom_writeXML('1', '0')
        cc.send(analysis_request_msg.encode('utf-8'))

        # 接收请求确认信息
        # 三次重传
        request_ack_msg = ''

        for i in range(3):
            try:
                cc.settimeout(10)
                request_ack_msg = cc.recv(100000).decode('utf-8')
                break
            except Exception:
                cc.send(analysis_request_msg.encode('utf-8'))
                # print(i)
        cc.settimeout(600)
        if request_ack_msg == '':
            print('未收到服务端的确认消息，已断开连接...')
            cc.close()

        # 解析请求确认信息
        dom.dom_readXML(request_ack_msg)
        if dom.getContentFlag() == '2':
            print('故障数据正在处理中...')
        else:
            print('请求确认消息格式错误，断开连接...')
            cc.close()

        # 等待接收数据分析完成消息
        while True:
            cc.settimeout(600)
            result_finish_msg = cc.recv(100000).decode('utf-8')

            if result_finish_msg == 'analyze_exit':
                date01 = datetime.date.today()
                result_path = str(date01.year) + "-" + str(date01.month) + "-" + str(date01.day)
                filename_output = '故障分析结果_' + train_num + '_' + transID
                ftp = FTPService().ftpConnect(IP, FTP_username, FTP_password)
                data_remotepath = '.\\output\\' + result_path + '\\' + filename_output + '.xml'
                data_localpath = '.\\output\\' + filename_output + '.xml'
                FTPService().downloadFile(ftp, data_remotepath, data_localpath)
                print('结果文件已下载...分析完成！')
                cc.close()
                break
            if result_finish_msg == '已断开连接...':
                print('分析过程出现错误。请重新输入...')
                cc.close()
                break
            dom.dom_readXML(result_finish_msg)
            if dom.getContentFlag() == '3':
                finish_recv_msg = dom.dom_writeXML('4', '0')
                cc.send(finish_recv_msg.encode('utf-8'))
            elif dom.getContentFlag() == '5':
                if dom.getReqResult() == '是否将该数据特征上传智能分类样本库':
                    msg_updata = False
                    send_msg = dom.dom_writeXML('5', str(msg_updata))
                    cc.send(send_msg.encode('utf-8'))

                else:
                    req_result = dom.getReqResult()
                    print(req_result)
            else:
                continue
    except Exception as e:
        print(e)
        if '远程主机强迫关闭了一个现有的连接' in str(e):
            print('远程主机强迫关闭了一个现有的连接。')
            return 0
        elif 'timed out' in str(e):
            print('连接等待超时，已断开连接...')
            return 0
        elif '由于目标计算机积极拒绝，无法连接' in str(e):
            print('无法连接服务器，请退出重启！')
            return 0
        else:
            print('出现错误，已断开连接!')
            return 0


def congifCheck(IP, port, FTP_username, FTP_password):
    if socket_link(IP, port) == 1:
        print('IP地址或端口号错误！')
        return 0
    elif FTPService().checkFTP(IP, FTP_username, FTP_password) == 1:
        print('FTP用户名或密码错误！')
        return 0
    else:
        return 1


def socket_link(IP, port):
    global dd
    try:
        socket.setdefaulttimeout(500)
        dd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # IP = var_IP.get()
        PORT = int(port)
        dd.connect((IP, PORT))
        dd.send('test'.encode('utf-8'))

        flag = 0
        dd.close()
    except Exception as e:
        print(e)
        flag = 1

    return flag