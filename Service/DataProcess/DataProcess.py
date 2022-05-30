import datetime


import os
import xlrd
import time

from Service.DataProcess.tezheng import aaaa
from Service.DataProcess.tezheng import abab
from Service.DataProcess.AI_test import Feature_Classify

from Service.DataProcess.ExcelRegenerate import ExcelRegenerate
from CTCSutils.DOM import *
# from ExcelProcess import getTimeByType
from Service.DataProcess.UnzipFile import UnZip
from Service.DataProcess.JDDataTransform import ooo
from Service.DataProcess.IntegrateExcel import IntegrateExcel
from Service.DataProcess.KeyWordSearch import KeyWordSearch

def analyRun(mysql_username, mysql_password, time_first, time_last, file_addr,x1, v, tranNum,transID,):
    global PRI_rowNum, Abis_rowNum, PRI_infodes, Abis_infodes, time_direc_tab, A_TZ, A_rowNum, XX, my_model, error_str, file_name_new

    if time_first == '':
        time_first_xml = '0'
    else:
        time_first_xml = time_first
    if time_last == '':
        time_last_xml = '0'
    else:
        time_last_xml = time_last
    now = datetime.now()

    dom = DOM(tranNum, now.strftime("%Y-%m-%d %H:%M:%S"), time_first_xml, time_last_xml, '5', transID, '')

    mysql_username = mysql_username
    mysql_password = mysql_password

    print('I.解压：' + file_addr)
    send_msg = dom.dom_writeXML('5', 'I.解压...\n')
    v.send(send_msg.encode('utf-8'))
    # isunzip = unzip_run('', file_addr)  # 解压程序
    isunzip = UnZip('').main('', file_addr)
    if isunzip:
        print('解压已完成\n')
        send_msg = dom.dom_writeXML('5', '解压已完成！\n')
        v.send(send_msg.encode('utf-8'))
    else:
        print('解压失败\n')
        send_msg = dom.dom_writeXML('5', '解压失败！\n')
        print(send_msg)
        v.send(send_msg.encode('utf-8'))

    send_msg = dom.dom_writeXML('5', 'II.统一化交大格式中\n')
    v.send(send_msg.encode('utf-8'))
    my_model = ''
    addr1 = os.getcwd()
    addr2 = file_addr.split('\\')[-1]
    addr3 = addr2.split('.')[0]  # CTCS3
    fileaddr_unzip = addr1 + '\\data\\unzip\\' + addr3
    for home, dirs, files in os.walk(fileaddr_unzip):
        for dir in dirs:
            if '交大' in dir:
                my_model = '交大'
                break
            elif '通号' in dir:
                my_model = '通号'
                break
            elif '铁科' in dir:
                my_model = '铁科'
                break
            else:
                my_model = ''
        break
    if my_model == '':
        send_msg = dom.dom_writeXML('5', '未找到数据格式！\n')
        v.send(send_msg.encode('utf-8'))
        raise Exception('未找到数据格式')
    else:
        pass

    print('II.统一化交大格式中\n')
    addr1 = os.getcwd()
    addr2 = file_addr.split('\\')[-1]
    addr3 = addr2.split('.')[0]  # CTCS3
    file_addr_unzip = addr1 + '\\data\\unzip\\' + addr3
    print(my_model)
    if my_model == '交大':
        for home, dirs, files in os.walk(file_addr_unzip):
            for file in files:
                file_addr_1 = home + '\\' + file
                file_name = os.path.split(file_addr_1)[1]
                if file_name.find('PRI') >= 0:
                    if file_name.find('信令') >= 0:
                        excel_file = xlrd.open_workbook(file_addr_1)
                        sheet1_content1 = excel_file.sheet_by_index(0)
                        a1 = sheet1_content1.cell(1, 3).value
                        a2 = sheet1_content1.cell(1, 4).value
                        if '车次号' in a1 and '小区信息' in a2:
                            print('该数据属于可用格式，不需要转化\n')
                            send_msg = dom.dom_writeXML('5', '该数据属于可用格式，不需要转化\n')
                            v.send(send_msg.encode('utf-8'))
                            break
                        else:
                            print('该类型不可用，正在转化为可用类型......\n\n')

                            send_msg = dom.dom_writeXML('5', '该类型不可用，正在转化为可用类型......\n\n')
                            v.send(send_msg.encode('utf-8'))

                            print('正在转化数据：' + file_addr)
                            # 交大数据的转化
                            a = ooo(file_addr_unzip)
                            if a == 1:
                                send_msg = dom.dom_writeXML('5', '\n Complete! \n')
                                v.send(send_msg.encode('utf-8'))
                                print('\n Complete! \n')
                                break
                            else:
                                print('该数据格式目前无法转化\n')
                                send_msg = dom.dom_writeXML('5', '该数据格式目前无法转化\n')
                                v.send(send_msg.encode('utf-8'))
                                break
        time.sleep(0.5)
    else:
        time.sleep(0.5)
    print('III.合并EXCEL中：' + file_addr)

    file_allname = os.path.split(file_addr)[1]
    file_name = os.path.splitext(file_allname)[0]
    send_msg = dom.dom_writeXML('5', 'III.合并EXCEL中.....\n')
    v.send(send_msg.encode('utf-8'))
    isintegrate = IntegrateExcel(my_model, file_name).main_integrate()
    # isintegrate = integrate_run(my_model, file_name)

    if isintegrate:
        print('合并已完成\n')
        send_msg = dom.dom_writeXML('5', '合并已完成\n')
        v.send(send_msg.encode('utf-8'))
    else:
        print('合并失败\n')
        send_msg = dom.dom_writeXML('5', '合并失败\n')
        v.send(send_msg.encode('utf-8'))
        raise Exception('合并失败')

    print('正在截取时间段：' + file_name + '\n')
    path_time = '.\\data\\download' + '\\' + file_name + '.xls'
    # if time_first == '' and time_last == '':
    #     try:
    #         time_first, time_last = getTimeByType(path_time, my_model)
    #     except Exception:
    #         time_first = ''
    #         time_last = ''
    # else:
    #     if int(time_last.split('-', -1)[4]) - int(time_first.split('-', -1)[4]) > 30:
    #         time_first = ''
    #         time_last = ''
    #     elif int(time_last.split('-', -1)[3]) - int(time_first.split('-', -1)[3]) == 1 and int(
    #             time_last.split('-', -1)[4]) + 60 - int(time_first.split('-', -1)[4]) > 30:
    #         time_first = ''
    #         time_last = ''
    file_name_new = file_name + '+' + time_first + '_to_' + time_last
    file_addr_new = '.\\data\\excel_time' + '\\' + file_name_new + '.xls'
    err_ip, empty_exl = ExcelRegenerate().excel_regenerate(my_model, path_time, time_first, time_last, file_addr_new)
    if err_ip == 0:
        print('输入时间大小错误,无法截取时间\n')
        send_msg = dom.dom_writeXML('5', '输入时间大小错误,无法截取时间\n')
        v.send(send_msg.encode('utf-8'))
        raise Exception('输入时间大小错误')
    if len(empty_exl) != 0:
        for ij in range(len(empty_exl)):
            print(empty_exl[ij] + '\n')
    print('时间段截取成功\n')
    print('正在对指定时间段数据进行处理...\n')

    print("V.搜索关键词中...")
    send_msg = dom.dom_writeXML('5', 'V.搜索关键词中...\n')
    v.send(send_msg.encode('utf-8'))
    t_output = time.strftime('%Y-%m-%d %H_%M_%S', time.localtime(time.time()))
    fw = open('.\\output\\' + t_output + ' + ' + file_name_new + ".txt", 'w', encoding="utf-8")
    try:
        issearch, time_direc_tab, PRI_rowNum, Abis_rowNum, A_rowNum, error_str = KeyWordSearch().search_run(my_model, file_name,
                                                                                            file_name_new,
                                                                                            mysql_username,
                                                                                            mysql_password)
        if issearch is None:
            print('搜索失败\n')
            send_msg = dom.dom_writeXML('5', '搜索失败\n')
            v.send(send_msg.encode('utf-8'))
        else:
            for str in issearch:
                for iii in range(len(time_direc_tab)):
                    fw.write("\n")
                    fw.write(time_direc_tab[iii])
                    fw.write("\n")
                if 'no data' in str:  # 不属于原因不明，属于没有数据的故障
                    print('[M] unclassified : ' + str + '\n')
                else:
                    print('[M] classified : ' + str + '\n')
                    msg = '[M] classified : ' + str
                    send_msg = dom.dom_writeXML('5', msg + '\n')
                    v.send(send_msg.encode('utf-8'))
            print('\n')
        print(file_addr)
    except Exception as e:
        print(e)
        time_direc_tab = []
        PRI_rowNum = []
        Abis_rowNum = []
        A_rowNum = []
        send_msg = dom.dom_writeXML('5', '关键词搜索失败！\n')
        v.send(send_msg.encode('utf-8'))
        print('关键词搜索失败！\n')


    if error_str == '不明原因' :
        try:
            print('IV.智能分类中...')
            send_msg = dom.dom_writeXML('5', 'IV.智能分类中...\n')
            v.send(send_msg.encode('utf-8'))
            path_jh = '.\data\excel_time'
            for home, dirs, files in os.walk(path_jh):
                for file in files:
                    file1 = file.split('.')[0]
                    if file1 == file_name_new:
                        a_tz, A_TZ, A_time_dire, PRI_rowNum, Abis_rowNum, PRI_infodes, Abis_infodes = aaaa(
                            file_addr_new, my_model)

                        if len(a_tz) != 0:
                            print('正在转化数据' + file_addr + '\n')
                            print('提取的特征分别为：\n')
                            for iik in range(len(A_time_dire)):
                                fw.write("\n")
                                fw.write(A_time_dire[iik])
                                fw.write("\n")
                            for iii in range(len(A_TZ)):
                                fw.write("\n")
                                print(A_TZ[iii] + '\n')
                                fw.write(A_TZ[iii])
                                fw.write("\n")
                            XX, B = Feature_Classify([a_tz])
                            a_tz.append(B)
                            print('********************************' + '\n')
                            print('该故障分类结果如下：\n')
                            print('决策树:' + XX[0] + '\n')
                            print('随机森林:' + XX[0] + '\n')
                            print('********************************' + '\n')

                            send_msg = dom.dom_writeXML('5', '********************************' + '\n' + '该故障分类结果如下：\n' + XX[
                                0] + '\n' + '********************************' + '\n')
                            v.send(send_msg.encode('utf-8'))
                            fw.write('\n该故障分类结果如下：\n')
                            fw.write(XX[0])
                            fw.write("\n")
                            fw.close()

                            send_msg = dom.dom_writeXML('5', '是否将该数据特征上传智能分类样本库')
                            v.send(send_msg.encode('utf-8'))

                            recv_msg = v.recv(100000).decode('utf-8')
                            # recv_msg = False
                            dom.dom_readXML(recv_msg)
                            msg_updata = dom.getReqResult()
                            print(msg_updata)
                            print(a_tz)
                            try:
                                if msg_updata == 'True':
                                    print('正在上传样本集\n\n')
                                    abab(a_tz)
                                else:
                                    print('数据可能不准确，暂不上传\n\n')
                            except:
                                break
                        else:
                            send_msg = dom.dom_writeXML('5', '该数据格式目前无法进行特征提取')
                            v.send(send_msg.encode('utf-8'))
                            print('该数据格式目前无法进行特征提取\n')
                            raise Exception('该数据格式目前无法进行特征提取')

        except Exception as e:
            print(e)

        print('Complete!\n')
        send_msg = dom.dom_writeXML('5', 'Complete!\n')
        v.send(send_msg.encode('utf-8'))

        return time_direc_tab, PRI_rowNum, Abis_rowNum, A_TZ, A_rowNum, XX, v
    else:
        XX = []
        A_TZ = []
        XX.append('无智能分析结果')
        A_TZ.append('无')
        return time_direc_tab, PRI_rowNum, Abis_rowNum, A_TZ, A_rowNum, XX, v


