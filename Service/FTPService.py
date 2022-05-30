# -*- coding: utf-8 -*-
"""
@File    :  FTPService.py
@Time    :  2022/3/30 17:53
@Author  :  changzheng
@Version :  1.0
@Desc    :  完成FTP的所有功能
"""
from ftplib import FTP

class FTPService:
    def __init__(self):
        pass

    def ftpConnect(self, host, username, password):
        ftp = FTP()
        ftp.connect(host, 21)
        ftp.encoding = 'gbk'
        ftp.login(username, password)
        return ftp


    def uploadFile(self, ftp, remotepath, localpath):
        bufsize = 1024
        file_handler = open(localpath, 'rb')
        # ftp = open(localpath, 'rb')
        ftp.storbinary('STOR ' + remotepath, file_handler, bufsize)
        # ftp.set_debuglevel(0)
        file_handler.close()
        return True

    def downloadFile(self, ftp, remotepath, localpath):
        bufsize = 1024
        file_handler = open(localpath, 'wb')
        print(file_handler)
        # self.ftp.retrbinary("RETR %s" % (RemoteFile), file_handler.write)#接收服务器上文件并写入本地文件
        ftp.retrbinary('RETR ' + remotepath, file_handler.write, bufsize)
        file_handler.close()
        return True

    def ftpClose(self,ftp):
        ftp.quit()

    def checkFTP(self,IP, FTP_username, FTP_password):
        global ftp
        try:
            ftp = self.ftpConnect(IP, FTP_username, FTP_password)
            flag = 0
            ftp.quit()
        except Exception as e:
            print(e)
            flag = 1