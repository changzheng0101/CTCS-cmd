import sys
import os
package_path = str(os.getcwd()) + r'\venv\Lib\site-packages'
sys.path.append(package_path)
import cmd
import os
import subprocess
from Service.ClientFunction import *

import warnings

warnings.filterwarnings("ignore")
global cc
class Query(cmd.Cmd):
    DATA_PATH = "./data"
    intro = '*****************************************\n'\
            '   CTCS-3级列控系统故障运维客户端v5.2    \n'\
            '*****************************************\n'\
            '输入 help 或者 ? 查看所有命令。\n' \
            'help + 命令 可查看命令具体解释'
    prompt = "CTCS>\n"
    doc_header = '可用命令为:'

    def __init__(self):  # 约定成俗这里应该使用r，它与self.r中的r同名
        super().__init__()
        self.IP = None
        self.PORT = None
        self.FTP_username = None
        self.FTP_password = None
        # self.socket = ''
        self.mysql_username = ''
        self.mysql_password = ''

    ######## 这是一个命令的完整示例 #########
    def do_login(self, args):
        'login+IP+PORT+FTP用户名+FTP密码，登录登录客户端'
        args_list = args.split()
        if len(args_list) != 4:
            print("输入格式错误，请重新输入!!!")
            return
        IP = args_list[0]
        PORT = args_list[1]
        FTP_username = args_list[2]
        FTP_password = args_list[3]

        check_flag = congifCheck(IP, PORT, FTP_username, FTP_password)
        if check_flag == 0:
            print('请重新登录！！！')
        elif check_flag == 1:
            print("*****************************\n"
                  "         登录成功...\n"
                  "*****************************\n")
            self.IP = IP
            self.PORT = PORT
            self.FTP_username = FTP_username
            self.FTP_password = FTP_password
        # denglu()

    def do_analyze(self, args):
        'analyze + 文件名 or anylyze + 文件名 + 起始时间 + 截止时间，完成对于文件的分析，可支持多参数'
        global time_first, time_last
        args_list = args.split()
        if len(args_list) == 1:
            time_first = ''
            time_last = ''
        elif len(args_list) == 3:
            time_first = args_list[1]
            time_last = args_list[2]
            pass
        else:
            print("输入格式错误，请重新输入!!!")
            return
        if '\\' in args:
            file_split = args_list[0].rsplit('\\', 1)[1].split('-', -1)
        else:
            file_split = args.split('-', -1)
        train_num = file_split[0]
        transID = file_split[1]
        IP = self.IP
        PORT = self.PORT
        FTP_username = self.FTP_username
        FTP_password = self.FTP_password


        if IP is None or PORT is None or FTP_username is None or FTP_password is None:
            print('请重新登录！！！')
            return
        else:
            try:
                socket.setdefaulttimeout(500)
                cc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # IP = var_IP.get()
                # PORT = int(var_duankou.get())
                cc.connect((IP, int(PORT)))
                # self.socket = cc
                analysisbtn(IP, cc, FTP_username, FTP_password, train_num, transID, args_list[0], time_first, time_last)
                cc.close()
            except Exception as e:
                print(e)
            return

    def do_query_by_id(self, args):
        'query_by_id + 车次号 + 服务端数据库用户名 + 服务端数据库密码，获取历史记录'
        global mysql_username, mysql_password, cc
        args_list = args.split()
        if len(args_list) != 3:
            print("输入格式错误，请重新输入!!!")
            return
        mysql_username = args_list[1]
        mysql_password = args_list[2]
        if self.IP is None and self.PORT is None:
            print('请先登录！！！')
            return
        # if self.socket == '':
        IP = self.IP
        PORT = self.PORT
        socket.setdefaulttimeout(500)
        cc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cc.connect((IP, int(PORT)))
        # self.socket = cc
        # else:
        #     cc = self.socket
        send_msg = 'query_by_id '+args
        cc.send(send_msg.encode('utf-8'))
        recv_msg = cc.recv(10000).decode('utf-8')
        print(recv_msg)
        cc.close()

    def do_accuracy(self, _):
        'accuracy, 查询服务器当前准确度'
        if self.IP is None and self.PORT is None:
            print('请先登录！！！')
            return
        IP = self.IP
        PORT = self.PORT
        socket.setdefaulttimeout(500)
        cc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cc.connect((IP, int(PORT)))
        cc.send('accuracy'.encode('utf-8'))
        accuracy_msg = cc.recv(10000).decode('utf-8')
        print(accuracy_msg)
        cc.close()
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
