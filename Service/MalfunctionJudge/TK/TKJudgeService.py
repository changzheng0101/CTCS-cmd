# -*- coding: utf-8 -*-
"""
@File    :  TKJudgeService.py
@Time    :  2022/3/31 19:44
@Author  :  changzheng
@Version :  1.0
@Desc    :  铁科故障的判断分类算法
"""
import os
import json
import xlrd

from Service.MalfunctionJudge.BaseJudgeService import BaseJudgeService
from Service.MalfunctionJudge.hs import *
from dateutil.parser import parse

class TKJudgeService(BaseJudgeService):
    def __init__(self, username, password):
        BaseJudgeService.__init__(self, username, password)
        pass

    def searchkey_updatesql(self, file_name, file_name_new, excel_inputpath, my_model):  # 搜索log数据函数
        global Update_db, row1, isChannelSwitch, PRI_time_value

        log_path = './data/unzip/' + '%s' % file_name
        # log_path_1 = './data/excel_time/' + '%s' % file_name_new

        # 调用配置文件中的时间
        RBC_NoApplicationLayer_Time = "RBC_NoApplicationLayer_Time"
        DI_Before_Time = "DI_Before_Time"
        NR_Time = "NR_Time"
        M39_Before_DISCONNECT_Time = "M39_Before_DISCONNECT_Time"
        with open("config.json") as fp:
            json_data = json.load(fp)

        # 打开数据
        excel_data = xlrd.open_workbook(excel_inputpath)
        single_excel = excel_data.sheet_by_index(0)  # 选择第一个sheet作为读取文件

        # 初始化
        global Update_db
        isChannelSwitch = ''
        Update_db = '不明原因'
        row = 0
        disc_error_3, frmr_error, di_error, m156_error, a_error, disc_error, row_break, disc_error_1, disc_error_2, msc_error, SIM_error_1, SIM_error_2 = False, False, False, False, False, False, False, False, False, False, False, False
        time_direc_tab = []
        dis = []
        dir = []
        Abis_dir = []
        Are = []
        row_length = single_excel.nrows
        col_length = single_excel.ncols
        mode = my_model
        PRI_rowNum = []
        A_rowNum = []
        Abis_rowNum = []
        NID_MESSAGE_list = []
        PRI_APDU_time_value = []
        NR_list = []
        PRI_NR_time_value = []


        # 打开数据，关键词搜索
        Update_db = self.log_search(log_path)
        row = 1
        row1 = 1
        regex1 = r"(\d\d\d)"
        regex2 = r"(\d\d)"
        if Update_db == '不明原因':
            for row in range(1, row_length):
                type_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'PRI_子类型_1'))
                data_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'PRI_数据内容_1'))
                PRI_index = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'PRI_序号_1'))
                # A_index = single_excel.cell_value(row, getColumnIndex(single_excel, 'A_序号_1'))
                Abis_index = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'Abis_序号_1'))
                um_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'A_Um类型_1'))
                direct_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'A_方向_1'))
                Areason_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'A_原因值_1'))
                SIM_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'Abis_Um类型_1'))
                Up_com_value = single_excel.cell_value(row,
                                                       self.getColumnIndex(single_excel, 'Abis_RxQualUp_2'))
                Down_com_value = single_excel.cell_value(row,
                                                         self.getColumnIndex(single_excel, 'Abis_RxQualDown_2'))
                Abis_direct_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'Abis_方向_1'))
                com_value = [Up_com_value, Down_com_value]
                PRI_time_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'PRI_触发时间_1'))
                PRI_dire_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'PRI_数据传送方向_1'))
                A_time_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'A_触发时间_1'))
                signalling_type_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'PRI_信令类型_1'))

                if signalling_type_value == 'APDU':
                    regex_result = re.findall(regex1, type_value)
                    if len(regex_result) == 0:
                        regex_result = re.findall(regex2, type_value)
                    NID_MESSAGE_list += regex_result
                    PRI_APDU_time_value.append(PRI_time_value)

                if PRI_dire_value == 'OBC->RBC':
                    OBCtoRBCValue1 = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'PRI_OBC->RBC_1'))
                    regex_NR = r"R\:(\d*)"
                    regex_result = re.findall(regex_NR, OBCtoRBCValue1)
                    NR_list += regex_result
                    if len(regex_result) != 0:
                        PRI_NR_time_value.append(PRI_time_value)

                if 'ER' == type_value or 'er' == type_value or ' ER' == type_value:
                    Update_db = 'er类型'
                    time_direc_tab.append(
                        '该故障类型为er类型，触发时间：' + PRI_time_value + '；触发方向：' + PRI_dire_value + '\n')
                    row1 = row
                    PRI_rowNum.append(int(PRI_index))
                    break
                elif 'IMSI DETACH INDICATION' in SIM_value or 'IMSI DETACH INDICATION' in um_value:
                    Update_db = 'sim卡脱网'
                    time_direc_tab.append(
                        '该故障类型为sim卡脱网，触发时间：' + PRI_time_value + '；触发方向：' + PRI_dire_value + '\n')
                    row1 = row
                    Abis_rowNum.append(int(Abis_index))
                    break
                elif 'FRMR' in type_value:
                    frmr_value = data_value.split()
                    Update_db = self.frmr_deepsearch(frmr_value, mode)
                    time_direc_tab.append(
                        '该故障类型为frmr类型，触发时间：' + PRI_time_value + '；触发方向：' + PRI_dire_value + '\n')
                    row1 = row
                    PRI_rowNum.append(int(PRI_index))
                    break
                elif 'DISCONNECT' in um_value and 'BSC<--MSC' in direct_value and 'BTS<--BSC' in Abis_direct_value and 'Radio interface failure' in Areason_value:
                    for ii in range(len(com_value)):
                        if com_value[ii] <= 4:
                            Update_db = 'msc类型'
                            time_direc_tab.append('该故障类型可能为msc类型，触发时间在：' + A_time_value + '附近；\n')
                            row1 = row
                            A_rowNum.append(row)
                            break
                elif 'SaPDU DI' == type_value:
                    if '10 00 00 ' in data_value or '10 00 00' in data_value or '10-00-00' in data_value:
                        for APDU_index in range(len(NID_MESSAGE_list)-1, -1, -1):
                            if int(NID_MESSAGE_list[APDU_index]) < 100:
                                if(parse('.'.join(PRI_time_value.rsplit(" ", 1)))-parse('.'.join(PRI_APDU_time_value[APDU_index].rsplit(" ", 1)))).seconds > json_data[RBC_NoApplicationLayer_Time]:
                                    Update_db = 'RBC不发应用层消息'
                                    break
                                if (parse('.'.join(PRI_time_value.rsplit(" ", 1))) - parse('.'.join(PRI_APDU_time_value[APDU_index].rsplit(" ", 1)))).seconds <= json_data[RBC_NoApplicationLayer_Time]:
                                    Update_db = '不明原因'
                                    break
                        if Update_db == 'RBC不发应用层消息':
                            Update_db = '不明原因'
                            break
                        if int(NID_MESSAGE_list[-1]) != 39 or ( int(NID_MESSAGE_list[-1]) == 39 and (parse('.'.join(PRI_time_value.rsplit(" ", 1)))-parse('.'.join(PRI_APDU_time_value[-1].rsplit(" ", 1)))).seconds > json_data[M39_Before_DISCONNECT_Time]):
                            NR_maxNum_time = []
                            NR_before_DI_Time_data = []
                            NR_before_DI_data = []
                            for NR_index in range(len(NR_list) - 1, -1, -1):
                                #  找DI之前20秒内数据
                                if (parse('.'.join(PRI_time_value.rsplit(" ", 1))) - parse('.'.join(PRI_NR_time_value[NR_index].rsplit(" ", 1)))).seconds <= json_data[DI_Before_Time]:
                                    NR_before_DI_data.append(NR_list[NR_index])
                                    NR_before_DI_Time_data.append(PRI_NR_time_value[NR_index])
                            NR_value_max = max(NR_before_DI_data, key=NR_before_DI_data.count)  # DI之前20秒次数出现最多的数据
                            for NR_before_DI_data_index in range(len(NR_before_DI_data)):
                                if NR_before_DI_data[NR_before_DI_data_index] == NR_value_max:
                                    NR_maxNum_time.append(
                                        NR_before_DI_Time_data[NR_before_DI_data_index])  # 获得出现次数最多的数据时间数据
                            # 判断第一时间与最后一个时间是否小于6秒
                            if (parse('.'.join(NR_maxNum_time[0].rsplit(" ", 1))) - parse('.'.join(NR_maxNum_time[-1].rsplit(" ", 1)))).seconds < json_data[NR_Time]:
                                Update_db = '安全层MAC'

                        if Update_db == '安全层MAC':
                            time_direc_tab.append(
                                '该故障类型为安全层MAC类型，触发时间：' + PRI_time_value + '；触发方向：' + PRI_dire_value + '\n')
                            row1 = row
                            PRI_rowNum.append(int(PRI_index))
                    elif '10 0a 05' in data_value or '10 0a 05 ' in data_value or '10-0a-05' in data_value or '11 0a 05 ' in data_value:
                        Update_db = 'SAPDU长度错误'
                        time_direc_tab.append(
                            '该故障类型为SAPDU长度错误类型，触发时间：' + PRI_time_value + '；触发方向：' + PRI_dire_value + '\n')
                        row1 = row
                        PRI_rowNum.append(int(PRI_index))
                    if Update_db != '不明原因':
                        break
                else:
                    Update_db = '不明原因'

            if Update_db != '不明原因':
                df = pd.read_excel(excel_inputpath)
                disc = []
                disc_time = []
                handover1 = []
                time_command = []
                handover2 = []
                time_complete = []
                handover3 = []
                time_failure = []
                handover4 = []
                time_chaoshi = []
                handover5 = []
                time_qhcs = []
                disc_um = []
                for i in range(row):
                    abis_umlx_1 = str(df.loc[i, 'Abis_Um类型_1'])
                    pri_zlx_1 = str(df.loc[i, 'PRI_信令类型_1'])
                    if 'DISCONNECT' in abis_umlx_1:
                        disc_um.append(i)
                    if 'DISC' in pri_zlx_1:
                        disc.append(i)
                        disc_time.append(str(df.loc[i, 'PRI_触发时间_1']))
                    if 'HANDOVER COMMAND' in abis_umlx_1:
                        handover1.append(i)
                        time_command.append(str(df.loc[i, 'Abis_触发时间_1']))
                    if 'HANDOVER COMPLETE' in abis_umlx_1:
                        handover2.append(i)
                        time_complete.append(str(df.loc[i, 'Abis_触发时间_1']))
                    if 'HANDOVER FAILURE' in abis_umlx_1:
                        handover3.append(i)
                        time_failure.append(str(df.loc[i, 'Abis_触发时间_1']))
                    if 'HANDOVER CONDITION INDICATION' in abis_umlx_1:
                        handover4.append(i)
                        time_chaoshi.append(str(df.loc[i, 'Abis_触发时间_1']))
                    if 'CONNECTION FAILURE INDICATION' in abis_umlx_1:
                        handover5.append(i)
                        time_qhcs.append(str(df.loc[i, 'Abis_触发时间_1']))

                for i in range(len(handover1)-1, -1, -1):
                    if len(time_command) > len(time_complete):
                        break
                    else:
                        timeOfFault = parse('.'.join(PRI_time_value.rsplit(" ", 1)))
                        timeCommand = parse('.'.join(time_command[i].rsplit(" ", 1)))
                        timeComplete = parse('.'.join(time_complete[i].rsplit(" ", 1)))
                        if (timeOfFault - timeCommand).days >= 0 and (timeComplete-timeOfFault).days >= 0:
                            isChannelSwitch = '小区切换'
                            break

                        elif (timeOfFault - timeCommand).days >= 0 and (timeOfFault - timeComplete).days >= 0:
                            isChannelSwitch = '非小区切换'
                        else:
                            isChannelSwitch = '非小区切换'


            for row in range(1, row_length):
                type_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'PRI_子类型_1'))
                um_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'A_Um类型_1'))
                direct_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'A_方向_1'))
                SIM_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'Abis_Um类型_1'))
                Up_com_value = single_excel.cell_value(row,
                                                       self.getColumnIndex(single_excel, 'Abis_RxQualUp_2'))
                Down_com_value = single_excel.cell_value(row,
                                                         self.getColumnIndex(single_excel, 'Abis_RxQualDown_2'))
                Abis_direct_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'Abis_方向_1'))
                PRI_time_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'PRI_触发时间_1'))
                PRI_dire_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'PRI_数据传送方向_1'))
                Abis_time_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'Abis_触发时间_1'))
                A_time_value = single_excel.cell_value(row, self.getColumnIndex(single_excel, 'A_触发时间_1'))
                A_clear_request = single_excel.cell_value(row,
                                                          self.getColumnIndex(single_excel, 'A_BSSMAP类型_1'))
                # A_index = single_excel.cell_value(row, getColumnIndex(single_excel, 'A_序号_1'))
                Abis_release_request = single_excel.cell_value(row,
                                                               self.getColumnIndex(single_excel, 'Abis_信令类型_1'))

                if 'DISCONNECT' in type_value or 'DISC' in type_value:
                    time_direc_tab.append(
                        'PRI接口发现DISCONNECT，触发时间：' + PRI_time_value + '；触发方向：' + PRI_dire_value + '\n')
                if 'DISCONNECT' in SIM_value or 'DISC' in SIM_value:
                    time_direc_tab.append(
                        'Abis接口发现DISCONNECT，触发时间：' + Abis_time_value + '；触发方向：' + Abis_direct_value + '\n')
                if 'RELEASE REQUEST' in Abis_release_request:
                    time_direc_tab.append(
                        'Abis接口发现RELEASE REQUEST，触发时间：' + Abis_time_value + '；触发方向：' + Abis_direct_value + '\n')
                if 'DISCONNECT' in um_value or 'DISC' in um_value:
                    time_direc_tab.append(
                        'A接口发现DISCONNECT，触发时间：' + A_time_value + '；触发方向：' + direct_value + '\n')
                    A_rowNum.append(row)
                if 'CLEAR REQUEST' in A_clear_request:
                    time_direc_tab.append(
                        'A接口发现CLEAR REQUEST，触发时间：' + A_time_value + '；触发方向：' + direct_value + '\n')
                    A_rowNum.append(row)

        # 连接分类数据库，存入关键行
        file_name_sql = file_name
        addr = '.\\data\\unzip\\' + file_name
        dirs = os.listdir(addr)
        for dir_name in dirs:
            if mode in dir_name:
                file_name_sql = dir_name
                break
        self.connectsql(Update_db)
        try:
            self.create_table(mode, file_name_sql)
        except Exception as e:
            print(e)
        cursor = self.connection.cursor()  # 获得游标

        field_content = ''
        values_content = ''
        error_no = ''
        if single_excel.nrows == 1:  # 插入数据前先判断是否有数据
            return 'no data'

        for col in range(col_length):
            key = single_excel.cell_value(0, col)  # 字段名
            values = str(single_excel.cell_value(row1, col))  # 转为字符串格式

            # if key == 'Unnamed: 254':
            #     key = 'Unnamed_254'
            if values == '':
                if col != 0:
                    for ii in range(row_length - row):
                        values = str(single_excel.cell_value(row + ii, col))
                        if values != '':
                            # values = values + "+%s" % ii
                            break
                        if ii == row_length - row - 1:
                            for jj in range(row):
                                values = str(single_excel.cell_value(row - jj, col))
                                if values != '':
                                    # values = values + "-%s" % jj
                                    break
                                if jj >= 200:
                                    break

            if 'PRI_序号_1' in key:  # 把错误位置的序号导出
                error_no = values
            # 错误的行导出
            if key == '' or key is None:
                pass
            else:
                values_content = values_content + "'" + values + "',"
                field_content = field_content + '`' + key + '`,'

        sql = 'insert into `' + file_name_sql + '` (' + field_content[:-1] + ') values(' + values_content[:-1] + ')'
        try:
            cursor.execute(sql)
            self.connection.commit()  # 运行数据库命令必须的语句
            self.connectexit()  # 关闭数据库
        except Exception as e:
            print(e)
        return Update_db, time_direc_tab, isChannelSwitch, PRI_rowNum, Abis_rowNum, A_rowNum

    def create_table(self, mode, list_name):
        global sql_field
        cursor = self.connection.cursor()  # 获得游标
        # 设置三个厂家数据字段的标准格式（统一数据格式标准）
        if mode == '铁科':
            sql_field = "` (`PRI_序号_1` varchar(20),`PRI_触发时间_1` varchar(50),`PRI_移动台MSISDN_1` varchar(20),`PRI_RBC号码_1` varchar(20),`PRI_机车号_1` varchar(20),`PRI_车次号_1` varchar(20),`PRI_数据传送方向_1` varchar(20)," \
                        "`PRI_CTCS_ID_1` varchar(20),`PRI_LRBG_1` varchar(20),`PRI_公里标_1` varchar(20),`PRI_速度_1` varchar(20),`PRI_信令类型_1` varchar(20),`PRI_子类型_1` varchar(100),`PRI_--_1` varchar(20),`PRI_OBC->RBC_1` varchar(20)," \
                        "`PRI_RBC->OBC_1` varchar(20),`PRI_CRC校验_1` varchar(20),`PRI_T_TRAIN_1` varchar(20),`PRI_设备号_1` varchar(20),`PRI_端口号_1` varchar(20),`PRI_时隙号_1` varchar(20),`PRI_数据长度_1` varchar(20),`PRI_数据内容_1` varchar(5120)," \
                        "`Abis_序号_1` varchar(20),`Abis_触发时间_1` varchar(50),`Abis_MSISDN_1` varchar(20),`Abis_用户识别码_1` varchar(20),`Abis_线路_1` varchar(20),`Abis_小区名称_1` varchar(20),`Abis_信令类型_1` varchar(50),`Abis_Um类型_1` varchar(50)," \
                        "`Abis_方向_1` varchar(20),`Abis_正反环_1` varchar(20),`Abis_BCCH_1` varchar(20),`Abis_公里标_1` varchar(20),`Abis_速度_1` varchar(20),`Abis_关键字_1` varchar(200),`Abis_端口号_1` varchar(20),`Abis_时隙号_1` varchar(20)," \
                        "`Abis_数据长度_1` varchar(20),`Abis_数据内容_1` varchar(300)," \
                        "`Abis_序号_2` varchar(20),`Abis_触发时间_2` varchar(50),`Abis_MSISDN_2` varchar(20),`Abis_用户识别码_2` varchar(20),`Abis_小区_2` varchar(20),`Abis_公里标_2` varchar(20),`Abis_速度_2` varchar(20),`Abis_BCCH_2` varchar(20)," \
                        "`Abis_RxLevelUp_2` varchar(20),`Abis_RxLevelDown_2` varchar(20),`Abis_RxQualUp_2` varchar(20),`Abis_RxQualDown_2` varchar(20),`Abis_TA_2` varchar(20),`Abis_NB0电平_2` varchar(20),`Abis_标记位_2` varchar(20)," \
                        "`Abis_频点999_2` varchar(20),`Abis_频点1000_2` varchar(20),`Abis_频点1001_2` varchar(20),`Abis_频点1002_2` varchar(20),`Abis_频点1003_2` varchar(20),`Abis_频点1004_2` varchar(20),`Abis_频点1005_2` varchar(20)," \
                        "`Abis_频点1006_2` varchar(20),`Abis_频点1007_2` varchar(20),`Abis_频点1008_2` varchar(20),`Abis_频点1009_2` varchar(20),`Abis_频点1010_2` varchar(20),`Abis_频点1011_2` varchar(20),`Abis_频点1012_2` varchar(20)," \
                        "`Abis_频点1013_2` varchar(20),`Abis_频点1014_2` varchar(20),`Abis_频点1015_2` varchar(20),`Abis_频点1016_2` varchar(20),`Abis_频点1017_2` varchar(20),`Abis_频点1018_2` varchar(20),`Abis_频点1019_2` varchar(20),`Abis_时间_2` varchar(20)," \
                        "`Abis_序号_3` varchar(20),`Abis_MSISDN_3` varchar(50),`Abis_用户识别码_3` varchar(20),`Abis_公里标_3` varchar(20),`Abis_速度_3` varchar(20),`Abis_切换前BCCH_3` varchar(20),`Abis_切换后BCCH_3` varchar(50),`Abis_切换前小区_3` varchar(50)," \
                        "`Abis_切换后小区_3` varchar(50),`Abis_切换前TCH_3` varchar(20),`Abis_切换后TCH_3` varchar(20),`Abis_切换发起时间_3` varchar(50),`Abis_切换结束时间_3` varchar(50),`Abis_切换执行时间_3` varchar(100)," \
                        "`Abis_切换结果_3` varchar(20),`Abis_切换原因_3` varchar(100),`Abis_切换失败原因_3` varchar(100)," \
                        "`A_机车号_1` varchar(20),`A_MSISDN_1` varchar(20),`A_用户识别码_1` varchar(20),`A_触发时间_1` varchar(50),`A_LAC_1` varchar(20),`A_CI_1` varchar(20),`A_小区名称_1` varchar(20),`A_SCCP类型_1` varchar(20),`A_BSSMAP/DTAP_1` varchar(20)," \
                        "`A_BSSMAP类型_1` varchar(100),`A_Um类型_1` varchar(100),`A_方向_1` varchar(20),`A_公里标_1` varchar(20),`A_原因值_1` varchar(500),`A_关键字_1` varchar(20)," \
                        "`A_切换开始时间_2` varchar(50),`A_切换结束时间_2` varchar(50),`A_切换状态_2` varchar(20),`A_LAC(源)_2` varchar(20),`A_CI(源)_2` varchar(20),`A_切换前小区名称_2` varchar(20),`A_LAC(目的)_2` varchar(20),`A_CI(目的)_2` varchar(20)," \
                        "`A_切换后小区名称_2` varchar(20),`A_切换是否成功_2` varchar(20),`A_切换原因_2` varchar(100),`A_切换失败原因_2` varchar(100),`A_公里标_2` varchar(20)" \
                        ") ENGINE=MyISAM DEFAULT CHARSET=utf8mb4"


        if isinstance(list_name, list):
            for str in list_name:
                # 创建数据表
                tablename = str
                # sql = "CREATE TABLE `" + tablename + "` (`pmid` int(11) NOT NULL,`article_title` varchar(1024) DEFAULT NULL,`author_list` varchar(1024) DEFAULT NULL,`abstract_text` text DEFAULT NULL,`keywords` varchar(1025) DEFAULT NULL,`pmcid` char(255) DEFAULT NULL,`pub_med_pub_date` char(255) DEFAULT NULL,`journal_issue` char(255) DEFAULT NULL,`serch_text` char(255) DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8"
                sql = "CREATE TABLE `" + tablename + sql_field  # 数据库创建表格
                cursor.execute("DROP TABLE IF EXISTS `" + tablename + "`")  # 若存在原表格则删除加入新表格
                cursor.execute(sql)  # 数据库语句执行
                self.connection.commit()
        else:
            tablename = list_name
            sql = "CREATE TABLE `" + tablename + sql_field
            cursor.execute("DROP TABLE IF EXISTS `" + tablename + "`")  # 发出警告
            cursor.execute(sql)
            self.connection.commit()