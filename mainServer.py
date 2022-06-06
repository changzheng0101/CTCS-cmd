import sys
import os
package_path = str(os.getcwd()) + r'\venv\Lib\site-packages'
sys.path.append(package_path)
import cmd
import os
import subprocess
from Service.ServerFunction import *
from Service.NetworkServerService import *
import threading
import socket

import warnings

warnings.filterwarnings("ignore")


class Query(cmd.Cmd):
    DATA_PATH = "./data"
    intro = '*****************************************\n'\
            '   CTCS-3级列控系统故障运维服务端v5.2    \n'\
            '*****************************************\n'\
            '输入 help 或者 ? 查看所有命令。\n' \
            'help + 命令 可查看命令具体解释'
    prompt = "CTCS>\n"
    doc_header = '可用命令为:'

    def __init__(self):
        super().__init__()
        self.username = None
        self.password = None
        self.IP = ''
        self.port = ''
        self.ftp_data = ''
        self.ftp_output = ''


    def do_login(self, args):
        'login+数据库用户名+数据库密码，登录数据库'
        args_list = args.split()
        if len(args_list) < 2:
            print("参数输入有误")
            return
        username = args_list[0]
        password = args_list[1]
        self.username = username
        self.password = password
        window_main(username, password)
        # denglu()

    def do_initial(self, args):
        'initial，初始化数据库'
        args_list = args.split()
        username = self.username
        password = self.password
        if username is None or password is None:
            print('请先登录！')
            return
        else:
            init_subwindow(username, password)
            return

    def do_startserver(self, args):
        'startserver + IP + port + ftp中data地址 + ftp中output地址，启动故障分析系统'
        if args == '':
            IP = ''
            port = ''
            ftp_data = ''
            ftp_output = ''
        else:
            args_list = args.split()
            if len(args_list) < 4:
                print("参数输入有误")
                return
            IP = args_list[0]
            port = args_list[1]
            ftp_data = args_list[2]
            ftp_output = args_list[3]
        mysql_username = self.username
        mysql_passowrd = self.password
        if IP == '' and port == '' and ftp_data == '' and ftp_output == '':
            IP = self.IP
            port = self.port
            ftp_data = self.ftp_data
            ftp_output = self.ftp_output
        elif IP != '' and port != '' and ftp_data != '' and ftp_output != '':
            self.IP = IP
            self.port = port
            self.ftp_data = ftp_data
            self.ftp_output = ftp_output

        if mysql_username is None or mysql_passowrd is None:
            print('请先登录！')
            return
        elif IP == '' or port == '':
            print('请输入IP地址或端口号！')
        elif ftp_data == '' or ftp_output == '':
            print('请输入ftp文件地址！')
        else:
            thread = threading.Thread(target=startServer,
                                      args=(IP, port, mysql_username, mysql_passowrd, ftp_data, ftp_output,))
            thread.daemon = True
            thread.start()
            # startServer(IP, port, mysql_username, mysql_passowrd, ftp_data, ftp_output)
            return

    def do_query_by_id(self, args):
        'query_by_id + 车次号 ，获取历史记录'
        global mysql_username, mysql_password, cc
        args_list = args.split()
        if len(args_list) != 1:
            print("输入格式错误，请重新输入!!!")
            return
        if self.username is None and self.password is None:
            print('请先登录！')
            return
        else:
            mysql_username = self.username
            mysql_password = self.password
        if self.IP == '' and self.port == '':
            print('请先开启服务器！！！')
            return
        IP = self.IP
        PORT = self.port
        username = self.username
        password = self.password
        args = args + ' ' + username + ' ' + password
        socket.setdefaulttimeout(500)
        try:
            cc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cc.connect((IP, int(PORT)))
            send_msg = 'query_by_id ' + args
            cc.send(send_msg.encode('utf-8'))
            recv_msg = cc.recv(10000).decode('utf-8')
            print(recv_msg)
            cc.close()
        except Exception:
            print('服务器断开，请先开启服务器！！！')


    def do_accuracy(self, args):
        'accuracy, 查询服务器当前准确度'
        if self.IP == '' and self.port == '':
            print('请先开启服务器！！！')
            return
        IP = self.IP
        PORT = self.port
        socket.setdefaulttimeout(500)
        try:
            cc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cc.connect((IP, int(PORT)))
            cc.send('accuracy'.encode('utf-8'))
            accuracy_msg = cc.recv(10000).decode('utf-8')
            print(accuracy_msg)
            cc.close()
        except Exception:
            print('服务器断开，请先开启服务器！！！')
        return

    def do_closeserver(self, args):
        'closeserver,关闭故障分析系统'
        IP = self.IP
        PORT = self.port
        if IP is None or PORT is None:
            print('请先登录！！！')
        try:
            cc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cc.connect((IP, int(PORT)))
            closeserver_msg = "close"
            cc.send(closeserver_msg.encode('utf-8'))
            msg = cc.recv(1000).decode('utf-8')
            if msg == 'OK':
                print('故障分析系统已关闭...')
            else:
                print('故障分析系统关闭失败...')
            cc.close()
        except Exception:
            print('故障分析系统关闭失败...')
        # close_window()
        return


    def complete_analyze(self, text, line, begidx, endidx):
        result = [item for item in os.listdir(self.DATA_PATH) if item.startswith(text) and item.find(".") != -1]
        if result:
            return result
        return '无法找到对应的提示文件'

    ###################修改这个之上的就行###################
    def do_help(self, arg):
        '帮助命令---> help + 命令 可查看命令具体解释'
        super().do_help(arg)

    def do_exit(self, _):
        '退出'
        exit(0)

    def emptyline(self):
        """import time
        空命令执行情况
        :return:  False 继续 True 退出
        """
        return False

    def default(self, line):
        """
        未知命令的情况,调用内置的进行判断
        :param line:
        :return:
        """
        subprocess.call(line.split(), shell=True)


if __name__ == '__main__':
    Query().cmdloop()
