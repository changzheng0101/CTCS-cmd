# -*- coding: utf-8 -*-
"""
@File    :  JDDataTransform.py
@Time    :  2022/4/3 10:05
@Author  :  zhuyugang
@Version :  1.0
@Desc    :  交大类型数据转换
"""

import os
import xlrd
import pandas as pd
from pandas import DataFrame


def ooo(path):  # 交大数据的转化
    path_1 = os.getcwd()
    #path_2 = path_1 + '\\data\\unzip'
    #rar = rarfile.RarFile(rArlist)
    #rar.extractall(path_2)
    aa = path.rsplit("\\", 1)[1]
    #aa = ab.split('.')[0]
    #path = path_2 + '\\' + aa
    start_dir = path_1 + '\\data'
    # 判断数据属于何种交大类型
    for home, dirs, files in os.walk(path):
        for file in files:
            file_addr = home + '\\' + file
            # excel_data = xlrd.open_workbook(file_addr)  # 打开excel文件
            file_name = os.path.split(file_addr)[1]
            if file_name.find('Abis') >= 0:
                if file_name.find('接口信令') >= 0:
                    ac = '交大1'
                    break
                elif file_name.find('信令') >= 0 and '接口信令' not in file_name:
                    excel_file = xlrd.open_workbook(file_addr)
                    sheet1_content1 = excel_file.sheet_by_index(0)
                    a1 = sheet1_content1.row_values(1)
                    if "序号" not in a1:
                        a1 = sheet1_content1.row_values(0)
                    if '原因值' in a1:
                        ac = '交大4'
                        break
                    elif'连接失败原因' in a1:
                        ac = '交大2'
                        break
                    else:
                        ac = None

    if ac == '交大1':
        for home, dirs, files in os.walk(path):
            for file in files:
                file_addr = home + '\\' + file
                excel_data = xlrd.open_workbook(file_addr)  # 打开excel文件
                file_name = os.path.split(file_addr)[1]
                if file_name.find('PRI') >= 0:
                    if file_name.find('信令') >= 0:
                        xl = pd.read_excel(file_addr, header=4)
                        xl.to_excel(file_addr + 'prixl.xls', sheet_name='sheet', index=False,
                                    header=True)
                        xl = pd.read_excel(file_addr + 'prixl.xls')
                        xl.drop('网络层类型', axis=1, inplace=True)  # 删除多余的字段
                        xl.drop('呼叫参考号', axis=1, inplace=True)
                        xl.drop('LRBG_ID', axis=1, inplace=True)
                        xl.drop('设备号', axis=1, inplace=True)
                        xl['小区信息'] = None  # 添加不存在的字段
                        xl['发送方T_Train'] = None
                        xl['探询帧标识'] = None
                        xl['IMSI'] = None
                        xl['LAC/CI'] = None
                        xl['应答器信息'] = None
                        xl['列控等级'] = None
                        xl['采集卡'] = None
                        order = ['序号', '触发时间', '方向', '车次号', '小区信息', 'CRC校验', '信令/C3数据类型', '链路层类型', '传输层类型',
                                 '安全层类型', '应用层类型', '发送方T_Train', 'T_Train', '车->地', '地->车', '探询帧标识', 'CTCS_ID',
                                 'MSISDN', 'IMSI', 'LAC/CI', '公里标', '中断原因', '速度', '机车号', 'RBC号码', 'RBC名称', '应答器信息',
                                 '路局号', '列控等级', '采集卡', '端口号', '时隙号', '数据长度', '数据内容']
                        xl = xl[order]  # 对数据进行重排
                        DataFrame(xl).to_excel(file_addr, sheet_name='Sheet1', index=False, header=True)
                        os.remove(file_addr + 'prixl.xls')
                    else:
                        xl = pd.read_excel(file_addr, header=4)
                        xl.to_excel(file_addr + 'prihj.xls', sheet_name='sheet', index=False,
                                    header=True)
                        xl = pd.read_excel(file_addr + 'prihj.xls')
                        xl.drop('设备号', axis=1, inplace=True)
                        xl.drop('呼叫参考', axis=1, inplace=True)
                        xl.drop('端口号', axis=1, inplace=True)
                        xl.drop('时隙号', axis=1, inplace=True)
                        xl['IMSI'] = None
                        xl['话单结果'] = None
                        xl['是否本局起呼'] = None
                        xl['起呼小区'] = None
                        xl['拆链小区'] = None
                        xl['起呼LAC/CI'] = None
                        xl['拆链LAC/CI'] = None
                        xl['起呼公里标'] = None
                        order = ['序号', 'IMSI', '主叫MSISDN', '机车号', '车次号', '车载ID', '被叫号码', '台帐名称', '起呼时间',
                                 '挂机时间', '业务时间', '呼叫类型', '释放原因', '话单结果', '是否本局起呼', '拆线发起方', '起呼小区',
                                 '拆链小区', '起呼LAC/CI', '拆链LAC/CI', '起呼公里标', '拆线公里标', '拆线速度']
                        xl = xl[order]
                        DataFrame(xl).to_excel(file_addr, sheet_name='Sheet1', index=False, header=True)
                        os.remove(file_addr + 'prihj.xls')
                elif file_name.find('Abis') >= 0:
                    if file_name.find('信令') >= 0:
                        xl = pd.read_excel(file_addr, header=4)
                        xl.to_excel(file_addr + 'abisxl.xls', sheet_name='sheet', index=False,
                                                   header=True)
                        xl = pd.read_excel(file_addr + 'abisxl.xls')
                        xl['车次号'] = None
                        xl['CTCS ID'] = None
                        xl['LAC/CI'] = None
                        xl['切换原因'] = None
                        xl['拆链原因'] = None
                        xl['TA'] = None
                        xl['RBC号码'] = None
                        xl['台账名称'] = None
                        xl['数据来源路局'] = None
                        xl['信道时隙'] = None
                        order = ['序号', '触发时间', '方向', 'IMSI', 'MSISDN', 'Um类型', '机车号', '车次号', '小区', 'CTCS ID', 'LAC/CI',
                                 '公里标', '信令类型', '切换原因', '拆链原因', '关键字', 'A类型', '速度', 'F-C-S', '信道时隙', 'TA', 'RBC号码',
                                 '台账名称', '数据来源路局', '设备号', '端口号', '时隙号', '正反环', '数据长度', '数据内容']
                        xl = xl[order]
                        DataFrame(xl).to_excel(file_addr, sheet_name='Sheet1', index=False, header=True)
                        os.remove(file_addr + 'abisxl.xls')
                    elif file_name.find('测量') >= 0:
                        xl = pd.read_excel(file_addr, header=4)
                        xl.to_excel(file_addr + 'abiscl.xls', sheet_name='sheet', index=False,
                                                   header=True)
                        xl = pd.read_excel(file_addr + 'abiscl.xls')
                        if 'AreaID' in xl:
                            xl.drop('AreaID', axis=1, inplace=True)
                        if 'AreaID' in xl:
                            xl.drop('AirTN', axis=1, inplace=True)
                        if 'CellID' in xl:
                            xl.drop('CellID', axis=1, inplace=True)
                        if 'NBCellArfcn1' in xl:
                            xl.drop('NBCellArfcn1', axis=1, inplace=True)
                        if 'NBCellArfcn1' in xl:
                            xl.drop('NBCellArfcn1', axis=1, inplace=True)
                        if 'NBCellArfcn3' in xl:
                            xl.drop('NBCellArfcn3', axis=1, inplace=True)
                        if 'NBCellArfcn4' in xl:
                            xl.drop('NBCellArfcn4', axis=1, inplace=True)
                        if 'NBCellArfcn5' in xl:
                            xl.drop('NBCellArfcn5', axis=1, inplace=True)
                        if '参考号' in xl:
                            xl.drop('参考号', axis=1, inplace=True)
                        xl['日期'] = None
                        xl['速度'] = None
                        xl['TCH'] = None
                        xl['测量报告类型'] = None
                        xl['1000'] = None
                        xl['1013'] = None
                        xl['1014'] = None
                        xl['1016'] = None
                        xl['车次号'] = None
                        xl['CTCS ID'] = None
                        xl['TA切换标识'] = None
                        order = ['序号', '日期', '触发时间', '小区', '公里标', '速度', '小区BCCH频率', 'TCH', '测量报告类型', '测量报告序号',
                                 'RxLevelUp', 'RxQualUp', 'RxLevelDown', 'RxQualDown', 'TA', 'NBCellArfcn0',
                                 'NBCellLevel0',
                                 '1000',
                                 'NBCellLevel1', 'NBCellLevel2', 'NBCellLevel3', 'NBCellLevel4', 'NBCellLevel5', '1013',
                                 '1014', '1016',
                                 'IMSI', 'MSISDN', '机车号', '车次号', 'CTCS ID', '设备编号', '端口号', '时隙号', 'RouteID', 'TA切换标识']
                        xl = xl[order]
                        DataFrame(xl).to_excel(file_addr, sheet_name='Sheet1', index=False, header=True)
                        os.remove(file_addr + 'abiscl.xls')
                    elif file_name.find('切换') >= 0:
                        xl = pd.read_excel(file_addr, header=4)
                        xl.to_excel(file_addr + 'abisqh.xls', sheet_name='sheet', index=False,
                                                   header=True)
                        xl = pd.read_excel(file_addr + 'abisqh.xls')
                        if '失败原因' not in xl:
                            xl['失败原因'] = None
                        if '速度' not in xl:
                            xl['速度'] = None
                        if '公里标' not in xl:
                            xl['公里标'] = None
                        xl['车次号'] = None
                        xl['CTCS ID'] = None
                        xl['RBC号码'] = None
                        xl['台账名称'] = None
                        xl['切换前LAC/CI'] = None
                        xl['切换后LAC/CI'] = None
                        xl['切后TA'] = None
                        order = ['序号', '开始时间', '结束时间', '执行时间', 'IMSI', 'MSISDN', '车次号', '机车号', 'CTCS ID', '切前小区',
                                 '切后小区', '切换结果', '发起原因', '失败原因', '公里标', '速度', 'RBC号码', '台账名称', '切换前LAC/CI',
                                 '切换后LAC/CI', '切前BCCH', '切后BCCH', '切前TCH', '切后TCH', '切前RxLevelUp', '切前RxQualUp',
                                 '切前RxLevelDown', '切前RxQualDown', '切后RxLevelDown', '切前TA', '切后TA']
                        xl = xl[order]
                        DataFrame(xl).to_excel(file_addr, sheet_name='Sheet1', index=False, header=True)
                        os.remove(file_addr + 'abisqh.xls')
                    else:
                        xl = pd.read_excel(file_addr, header=4)
                        xl.to_excel(file_addr + 'abishj.xls', sheet_name='sheet', index=False,
                                                   header=True)
                        xl = pd.read_excel(file_addr + 'abishj.xls')
                        xl.drop('被叫号码', axis=1, inplace=True)
                        xl.drop('TCH', axis=1, inplace=True)
                        xl['台账名称'] = None
                        xl['起呼LAC/CI'] = None
                        xl['拆链LAC/CI'] = None
                        xl['拆链载频'] = None
                        xl['起呼公里标'] = None
                        order = ['序号', 'IMSI', 'MSISDN', '机车号', '车次号', 'CTCS_ID', '主叫号码', '台账名称', '起呼时间',
                                 '中断时间', '持续时间', '业务类型', '起呼小区', '拆线小区', '起呼LAC/CI', '拆链LAC/CI', '起呼载频',
                                 '拆链载频', '拆线方', '拆线情况', '起呼公里标', '拆线公里标', '拆线速度']
                        xl = xl[order]
                        DataFrame(xl).to_excel(file_addr, sheet_name='Sheet1', index=False, header=True)
                        os.remove(file_addr + 'abishj.xls')
                else:
                    if file_name.find('信令') >= 0:
                        xl = pd.read_excel(file_addr, header=4)
                        xl.to_excel(file_addr + 'axl.xls', sheet_name='sheet', index=False,
                                    header=True)
                        xl = pd.read_excel(file_addr + 'axl.xls')
                        xl.drop('原因值2', axis=1, inplace=True)
                        xl['CTCS ID'] = None
                        xl['小区信息'] = None
                        xl['LAC/CI'] = None
                        xl['RBC号码'] = None
                        xl['CIC'] = None
                        xl['应答器编号'] = None
                        xl['数据来源路局'] = None
                        xl['采集卡'] = None
                        xl['信令内容'] = None
                        order = ['序号', '触发时间', '方向', 'IMSI', 'MSISDN', '机车号', '车次号', 'CTCS ID',
                                 '小区信息', 'LAC/CI', '公里标', 'SCCP信息类型', 'BSSMAP_DTAP_IND', 'BSSMAP信息类型',
                                 'Um信息类型', '原因值1', '无线资源原因值', '速度', 'RBC号码', '采集设备', 'BSC', 'CIC',
                                 '应答器编号', '数据来源路局', '采集卡', '端口号', '时隙号', '数据长度', '信令内容']
                        xl = xl[order]
                        DataFrame(xl).to_excel(file_addr, sheet_name='Sheet1', index=False, header=True)
                        os.remove(file_addr + 'axl.xls')
                    elif file_name.find('切换') >= 0:
                        xl = pd.read_excel(file_addr, header=4)
                        xl.to_excel(file_addr + 'aqh.xls', sheet_name='sheet', index=False,
                                    header=True)
                        xl = pd.read_excel(file_addr + 'aqh.xls')
                        xl.drop('切换前BTS', axis=1, inplace=True)
                        xl.drop('切换后BTS', axis=1, inplace=True)
                        xl['执行时间'] = None
                        xl['车次号'] = None
                        xl['CTCS ID'] = None
                        xl['RBC号码'] = None
                        xl['台账名称'] = None
                        xl['切换目标小区'] = None
                        xl['切换前小区'] = None
                        xl['切换前LAC/CI'] = None
                        xl['切换后LAC/CI'] = None
                        xl['切换前CIC'] = None
                        xl['切换后CIC'] = None
                        order = ['序号', '开始时间', '结束时间', '执行时间', 'IMSI', 'MSISDN', '机车号', '车次号', 'CTCS ID',
                                 'RBC号码', '台账名称', '触发切换原因值', '切换结果', '切换失败原因值', '切换目标小区', '无线资源原因值',
                                 '公里标', '速度', '切换前采集设备', '切换后采集设备', '切换前BSC', '切换后BSC', '切换前小区',
                                 '切换前LAC/CI', '切换后LAC/CI', '切换前BCCH', '切换后BCCH', '切换前CIC', '切换后CIC']
                        xl = xl[order]
                        DataFrame(xl).to_excel(file_addr, sheet_name='Sheet1', index=False, header=True)
                        os.remove(file_addr + 'aqh.xls')
                    else:
                        xl = pd.read_excel(file_addr, header=4)
                        xl.to_excel(file_addr + 'ahj.xls', sheet_name='sheet', index=False,
                                    header=True)
                        xl = pd.read_excel(file_addr + 'ahj.xls')
                        xl.drop('被叫号码', axis=1, inplace=True)
                        xl.drop('DiscCause', axis=1, inplace=True)
                        xl.drop('CleanCause', axis=1, inplace=True)
                        xl['车次号'] = None
                        xl['CTCS ID'] = None
                        xl['台账名称'] = None
                        xl['业务时长'] = None
                        xl['起呼LAC/CI'] = None
                        xl['拆链LAC/CI'] = None
                        xl['起呼公里标'] = None
                        order = ['序号', 'IMSI', 'MSISDN', '机车号', '车次号', 'CTCS ID', '主叫号码', '台账名称', '起呼时间',
                                 '拆线时间', '业务时长', '起呼小区', '拆线小区', '起呼LAC/CI', '拆链LAC/CI', '类型', '拆线发起方',
                                 '起呼公里标', '拆线公里标', '拆线速度', '拆线原因', '清除原因']
                        xl = xl[order]
                        DataFrame(xl).to_excel(file_addr, sheet_name='Sheet1', index=False, header=True)
                        os.remove(file_addr + 'ahj.xls')
        #zip_norm(start_dir, aa, path)
        #shutil.rmtree(path)
        return 1
    elif ac == '交大2':
        for home, dirs, files in os.walk(path):
            for file in files:
                file_addr = home + '\\' + file
                excel_data = xlrd.open_workbook(file_addr)  # 打开excel文件
                file_name = os.path.split(file_addr)[1]
                if file_name.find('PRI') >= 0:
                    if file_name.find('信令') >= 0:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            if '运行模式' in xl:
                                xl.rename(index=str, columns={"运行模式": "列控等级"}, inplace=True)
                            order = ['序号', '记录时间', '信令方向', '车次号', '小区信息', 'CRC校验', '帧类型', '链路层', '传输层',
                                     '安全层', '应用层', '发送方T_Train', '确认对方T_Train', '车->地', '地->车', '探询帧标识', 'CTCS ID',
                                     'MSISDN', 'IMSI', 'LAC/CI', '公里标', '拆链原因', '速度(km/h)', '机车号', 'RBC号码', '台账名称',
                                     '应答器信息',
                                     '数据来源路局', '列控等级', '采集卡', '端口', '时隙', '信令长度', '信令内容']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'prixl%s.xlsx' %i, sheet_name='sheet', index=False,
                                                   header=True)
                        os.remove(file_addr)
                    else:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            order = ['序号', 'IMSI', '主叫MSISDN', '机车号', '车次号', 'CTCS ID', 'RBC号码', '台帐名称', '起呼时间',
                                     '挂机时间', '业务时长', '呼叫类型', '拆链原因', '话单结果', '是否本局起呼', '拆链发起方', '起呼小区',
                                     '拆链小区', '起呼LAC/CI', '拆链LAC/CI', '起呼公里标', '拆链公里标', '拆链速度(km/h)']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'prihj%s.xlsx' %i, sheet_name='sheet', index=False,
                                                   header=True)
                        os.remove(file_addr)
                elif file_name.find('Abis') >= 0:
                    if file_name.find('信令') >= 0:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            order = ['序号', '记录时间', '信令方向', 'IMSI', 'MSISDN', 'Um类型', '机车号', '车次号', '小区信息', 'CTCS ID',
                                     'LAC/CI',
                                     '公里标', '信令类型', '切换原因', '拆链原因', '无线资源原因值', '连接失败原因', '速度(km/h)', 'TCH', '信道时隙',
                                     'TA', 'RBC号码',
                                     '台账名称', '数据来源路局', '采集卡', '端口', '时隙', '正反环', '信令长度', '信令内容']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'abisxl%s.xlsx' %i, sheet_name='sheet', index=False,
                                                   header=True)
                        os.remove(file_addr)
                    elif file_name.find('测量') >= 0:
                        xl = pd.read_excel(file_addr, header=41, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=41, sheet_name='%s'%count[i])
                            if '车次号' not in xl:
                                xl = pd.read_excel(file_addr, header=42, sheet_name='%s'%count[i])
                            if '时间' in xl:
                                xl.rename(index=str, columns={"时间": "日期"}, inplace=True)
                            xl.drop('1003', axis=1, inplace=True)
                            xl.drop('1009', axis=1, inplace=True)
                            xl.drop('1011', axis=1, inplace=True)
                            xl.drop('1012', axis=1, inplace=True)
                            xl.drop('1017', axis=1, inplace=True)
                            order = ['序号', '日期', '触发时间', '小区信息', '公里标', '速度(km/h)', 'BCCH', 'TCH', '测量报告类型', '测量报告序号',
                                     '上行接收电平(dBm)', '上行接收质量', '下行接收电平(dBm)', '下行接收质量', 'TA', 'NB0Arfcn',
                                     'NB0Level(dBm)',
                                     '1000',
                                     '1001', '1002', '1005', '1007', '1008', '1013',
                                     '1014', '1016',
                                     'IMSI', 'MSISDN', '机车号', '车次号', 'CTCS ID', '采集卡', '端口', '时隙', '切换标识', 'TA切换标识']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'abiscl%s.xlsx' % i, sheet_name='sheet', index=False,
                                                   header=True)
                        os.remove(file_addr)
                    elif file_name.find('切换') >= 0:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            if '切换后上行电平-质量' in xl:
                                xl.drop('切换后上行电平-质量', axis=1, inplace=True)
                            xl['切换前BCCH'] = None
                            xl['切换前TCH'] = None
                            xl['切换前上行电平'] = None
                            xl['切换前下行电平'] = None
                            xl['切换前TA'] = None
                            order = ['序号', '开始时间', '结束时间', '执行时间', 'IMSI', 'MSISDN', '车次号', '机车号', 'CTCS ID', '切换前小区',
                                     '切换目标小区', '切换结果', '触发切换原因', '切换失败原因', '公里标', '速度(km/h)', 'RBC号码', '台账名称',
                                     '切换前LAC/CI',
                                     '切换后LAC/CI', '切换前BCCH', '切换前后BCCH', '切换前TCH', '切换前后TCH', '切换前上行电平', '切换前上行电平-质量',
                                     '切换前下行电平', '切换前下行电平-质量', '切换后下行电平-质量', '切换前TA', '切换前后TA']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'abisqh%s.xlsx' %i, sheet_name='sheet' + str(i), index=False,
                                                   header=True)
                        os.remove(file_addr)
                    else:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            order = ['序号', 'IMSI', '主叫MSISDN', '机车号', '车次号', 'CTCS ID', 'RBC号码', '台账名称', '起呼时间',
                                     '拆链时间', '业务时长', '呼叫类型', '起呼小区', '拆链小区', '起呼LAC/CI', '拆链LAC/CI', '起呼载频',
                                     '拆链载频', '拆链发起方', '拆链原因', '起呼公里标', '拆链公里标', '拆链速度(km/h)']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'abishj%s.xlsx' %i, sheet_name='sheet' + str(i), index=False,
                                                   header=True)
                        os.remove(file_addr)
                else:
                    if file_name.find('信令') >= 0:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            if '应答器信息' in xl:
                                xl.rename(index=str, columns={"应答器信息": "应答器编号"}, inplace=True)
                            order = ['序号', '记录时间', '信令方向', 'IMSI', 'MSISDN', '机车号', '车次号', 'CTCS ID',
                                     '小区信息', 'LAC/CI', '公里标', 'SCCP信令类型', 'BSSMAP_DTAP_IND', 'BSSMAP信息类型',
                                     'Um信息类型', '原因值', '无线资源原因值', '速度(km/h)', 'RBC号码', '台账名称', 'BSC', 'CIC',
                                     '应答器编号', '数据来源路局', '采集卡', '端口', '时隙', '信令长度', '信令内容']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'axl%s.xlsx' %i, sheet_name='sheet' + str(i), index=False,
                                                   header=True)
                        os.remove(file_addr)
                    elif file_name.find('切换') >= 0:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            order = ['序号', '开始时间', '结束时间', '执行时间', 'IMSI', 'MSISDN', '机车号', '车次号', 'CTCS ID',
                                     'RBC号码', '台账名称', '触发切换原因', '切换结果', '切换失败原因', '切换目标小区', '无线资源原因',
                                     '公里标', '速度(km/h)', '切换前采集设备', '切换后采集设备', '切换前BSC', '切换后BSC', '切换前小区',
                                     '切换前LAC/CI', '切换后LAC/CI', '切换前BCCH', '切换后BCCH', '切换前CIC', '切换后CIC']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'aqh%s.xlsx' %i, sheet_name='sheet' + str(i), index=False,
                                                   header=True)
                        os.remove(file_addr)
                    else:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            order = ['序号', 'IMSI', '主叫MSISDN', '机车号', '车次号', 'CTCS ID', 'RBC号码', '台账名称', '起呼时间',
                                     '拆链时间', '业务时长', '起呼小区', '拆链小区', '起呼LAC/CI', '拆链LAC/CI', '呼叫类型', '拆链发起方',
                                     '起呼公里标', '拆链公里标', '拆链速度(km/h)', '拆链原因', '清除原因']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'ahj%s.xlsx' %i, sheet_name='sheet' + str(i), index=False,
                                                   header=True)
                        os.remove(file_addr)
        #zip_norm(start_dir, aa, path)
        #shutil.rmtree(path)
        return 1
    elif ac == '交大4':
        for home, dirs, files in os.walk(path):
            for file in files:
                file_addr = home + '\\' + file
                excel_data = xlrd.open_workbook(file_addr)  # 打开excel文件
                file_name = os.path.split(file_addr)[1]
                if file_name.find('PRI') >= 0:
                    if file_name.find('信令') >= 0:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            if '车次号' not in xl:
                                xl = pd.read_excel(file_addr, header=0, sheet_name='%s'%count[i])
                            if 'RR帧类型' in xl:
                                xl.drop('RR帧类型', axis=1, inplace=True)
                                xl['探询帧标识'] = None
                                if '运行模式' in xl:
                                    xl.rename(index=str, columns={"运行模式": "列控等级"}, inplace=True)
                            order = ['序号', '记录时间', '信令方向', '车次号', '小区信息', 'CRC校验', '帧类型', '链路层', '传输层',
                                     '安全层', '应用层', '发送方T_Train', '确认对方T_Train', '车->地', '地->车', '探询帧标识', 'CTCS ID',
                                     'MSISDN', 'IMSI', 'LAC/CI', '公里标', '拆链原因', '速度(km/h)', '机车号', 'RBC号码', '台账名称',
                                     '应答器信息',
                                     '数据来源路局', '列控等级', '采集卡', '端口', '时隙', '信令长度', '信令内容']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'prixl%s.xlsx' %i, sheet_name='sheet', index=False,
                                                   header=True)
                        os.remove(file_addr)
                    else:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            order = ['序号', 'IMSI', '主叫MSISDN', '机车号', '车次号', 'CTCS ID', 'RBC号码', '台账名称', '起呼时间',
                                     '挂机时间', '业务时长', '呼叫类型', '拆链原因', '话单结果', '是否本局起呼', '拆链发起方', '起呼小区',
                                     '拆链小区', '起呼LAC/CI', '拆链LAC/CI', '起呼公里标', '拆链公里标', '拆链速度(km/h)']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'prihj%s.xlsx' %i, sheet_name='sheet', index=False,
                                                   header=True)
                        os.remove(file_addr)
                elif file_name.find('Abis') >= 0:
                    if file_name.find('信令') >= 0:
                        header_num = 1
                        xl = pd.read_excel(file_addr, header=header_num, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=header_num, sheet_name='%s' % count[i])
                            if '序号' not in xl:
                                header_num = 0
                                xl = pd.read_excel(file_addr, header=header_num, sheet_name='%s' % count[i])
                            order = ['序号', '记录时间', '信令方向', 'IMSI', 'MSISDN', 'Um类型', '机车号', '车次号', '小区信息', 'CTCS ID',
                                     'LAC/CI',
                                     '公里标', '信令类型', '切换原因', '拆链原因', '无线资源原因值', '原因值', '速度(km/h)', 'TCH', '信道时隙',
                                     'TA', 'RBC号码',
                                     '台账名称', '数据来源路局', '采集卡', '端口', '时隙', '正反环', '信令长度', '信令内容']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'abisxl%s.xlsx' %i, sheet_name='sheet', index=False,
                                                   header=True)
                        os.remove(file_addr)
                    elif file_name.find('测量') >= 0:
                        xl = pd.read_excel(file_addr, header=41, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=41, sheet_name='%s'%count[i])
                            if '车次号' not in xl:
                                xl = pd.read_excel(file_addr, header=42, sheet_name='%s'%count[i])
                                if '车次号' not in xl:
                                    xl = pd.read_excel(file_addr, header=0, sheet_name='%s' % count[i])
                            if '时间' in xl:
                                xl.rename(index=str, columns={"时间": "日期"}, inplace=True)
                            if '1000' in xl:
                                xl.drop('1000', axis=1, inplace=True)
                            if '1001' in xl:
                                xl.drop('1001', axis=1, inplace=True)
                            if '1002' in xl:
                                xl.drop('1002', axis=1, inplace=True)
                            if '1003' in xl:
                                xl.drop('1003', axis=1, inplace=True)
                            if '1004' in xl:
                                xl.drop('1004', axis=1, inplace=True)
                            if '1005' in xl:
                                xl.drop('1005', axis=1, inplace=True)
                            if '1006' in xl:
                                xl.drop('1006', axis=1, inplace=True)
                            if '1007' in xl:
                                xl.drop('1007', axis=1, inplace=True)
                            if '1008' in xl:
                                xl.drop('1008', axis=1, inplace=True)
                            if '1009' in xl:
                                xl.drop('1009', axis=1, inplace=True)
                            if '1010' in xl:
                                xl.drop('1010', axis=1, inplace=True)
                            if '1011' in xl:
                                xl.drop('1011', axis=1, inplace=True)
                            if '1012' in xl:
                                xl.drop('1012', axis=1, inplace=True)
                            if '1013' in xl:
                                xl.drop('1013', axis=1, inplace=True)
                            if '1014' in xl:
                                xl.drop('1014', axis=1, inplace=True)
                            if '1015' in xl:
                                xl.drop('1015', axis=1, inplace=True)
                            if '1016' in xl:
                                xl.drop('1016', axis=1, inplace=True)
                            if '1017' in xl:
                                xl.drop('1017', axis=1, inplace=True)
                            if '1018' in xl:
                                xl.drop('1018', axis=1, inplace=True)
                            if '时间' in xl:
                                xl.drop('时间', axis=1, inplace=True)
                                xl['日期'] = None
                            xl['1000'] = None
                            xl['1001'] = None
                            xl['1003'] = None
                            xl['1005'] = None
                            xl['1007'] = None
                            xl['1008'] = None
                            xl['1013'] = None
                            xl['1014'] = None
                            xl['1016'] = None
                            order = ['序号', '日期', '触发时间', '小区信息', '公里标', '速度(km/h)', 'BCCH', 'TCH', '测量报告类型', '测量报告序号',
                                     '上行接收电平(dBm)', '上行接收质量', '下行接收电平(dBm)', '下行接收质量', 'TA', 'NB0Arfcn',
                                     'NB0Level(dBm)',
                                     '1000',
                                     '1001', '1003', '1005', '1007', '1008', '1013',
                                     '1014', '1016',
                                     'IMSI', 'MSISDN', '机车号', '车次号', 'CTCS ID', '采集卡', '端口', '时隙', '切换标识', 'TA切换标识']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'abiscl%s.xlsx' % i, sheet_name='sheet', index=False,
                                                   header=True)
                        os.remove(file_addr)
                    elif file_name.find('切换') >= 0:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            xl.drop('切换后上行电平-质量', axis=1, inplace=True)
                            xl['切换后BCCH'] = None
                            xl['切换后BCCH'].fillna(xl['切换前后BCCH'])
                            xl['切换后TCH'] = None
                            xl['切换后TCH'].fillna(xl['切换前后TCH'])
                            xl['切换前上行质量'] = None
                            xl['切换前上行质量'].fillna(xl['切换前上行电平-质量'])
                            xl['切换前下行质量'] = None
                            xl['切换前下行质量'].fillna(xl['切换前下行电平-质量'])
                            xl['切换后TA'] = None
                            xl['切换后TA'].fillna(xl['切换前后BCCH'])
                            order = ['序号', '开始时间', '结束时间', '执行时间', 'IMSI', 'MSISDN', '车次号', '机车号', 'CTCS ID', '切换前小区',
                                     '切换目标小区', '切换结果', '切换原因', '切换失败原因', '公里标', '速度(km/h)', 'RBC号码', '台账名称',
                                     '切换前LAC/CI',
                                     '切换后LAC/CI', '切换前后BCCH', '切换后BCCH', '切换前后TCH', '切换后TCH', '切换前上行电平-质量', '切换前上行质量',
                                     '切换前下行电平-质量', '切换前下行质量', '切换后下行电平-质量', '切换前后TA', '切换后TA']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'abisqh%s.xlsx' %i, sheet_name='sheet' + str(i), index=False,
                                                   header=True)
                        os.remove(file_addr)
                    else:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            order = ['序号', 'IMSI', '主叫MSISDN', '机车号', '车次号', 'CTCS ID', 'RBC号码', '台账名称', '起呼时间',
                                     '拆链时间', '业务时长', '呼叫类型', '起呼小区', '拆链小区', '起呼LAC/CI', '拆链LAC/CI', '起呼载频',
                                     '拆链载频', '拆链发起方', '拆链原因', '起呼公里标', '拆链公里标', '拆链速度(km/h)']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'abishj%s.xlsx' %i, sheet_name='sheet' + str(i), index=False,
                                                   header=True)
                        os.remove(file_addr)
                else:
                    if file_name.find('信令') >= 0:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            if '应答器信息' in xl:
                                xl.rename(index=str, columns={"应答器信息": "应答器编号"}, inplace=True)
                            order = ['序号', '记录时间', '信令方向', 'IMSI', 'MSISDN', '机车号', '车次号', 'CTCS ID',
                                     '小区信息', 'LAC/CI', '公里标', 'SCCP信令类型', 'BSSMAP_DTAP_IND', 'BSSMAP信息类型',
                                     'Um信息类型', '原因值', '无线资源原因值', '速度(km/h)', 'RBC号码', '台账名称', 'BSC', 'CIC',
                                     '应答器编号', '数据来源路局', '采集卡', '端口', '时隙', '信令长度', '信令内容']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'axl%s.xlsx' %i, sheet_name='sheet' + str(i), index=False,
                                                   header=True)
                        os.remove(file_addr)
                    elif file_name.find('切换') >= 0:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            order = ['序号', '开始时间', '结束时间', '执行时间', 'IMSI', 'MSISDN', '机车号', '车次号', 'CTCS ID',
                                     'RBC号码', '台账名称', '切换原因', '切换结果', '切换失败原因', '切换目标小区', '无线资源原因',
                                     '公里标', '速度(km/h)', '切换前采集设备', '切换后采集设备', '切换前BSC', '切换后BSC', '切换前小区',
                                     '切换前LAC/CI', '切换后LAC/CI', '切换前BCCH', '切换后BCCH', '切换前CIC', '切换后CIC']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'aqh%s.xlsx' %i, sheet_name='sheet' + str(i), index=False,
                                                   header=True)
                        os.remove(file_addr)
                    elif file_name.find('呼叫') >= 0:
                        xl = pd.read_excel(file_addr, header=1, sheet_name=None)
                        count = list(xl)
                        for i in range(len(count)):
                            xl = pd.read_excel(file_addr, header=1, sheet_name='%s'%count[i])
                            order = ['序号', 'IMSI', '主叫MSISDN', '机车号', '车次号', 'CTCS ID', 'RBC号码', '台账名称', '起呼时间',
                                     '拆链时间', '业务时长', '起呼小区', '拆链小区', '起呼LAC/CI', '拆链LAC/CI', '呼叫类型', '拆链发起方',
                                     '起呼公里标', '拆链公里标', '拆链速度(km/h)', '拆链原因', '清除原因']
                            xl = xl[order]
                            DataFrame(xl).to_excel(file_addr + 'ahj%s.xlsx' %i, sheet_name='sheet' + str(i), index=False,
                                                   header=True)
                        os.remove(file_addr)
        #zip_norm(start_dir, aa, path)
        return 1
    else:
        return 0