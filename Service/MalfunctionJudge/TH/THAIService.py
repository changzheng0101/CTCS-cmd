# -*- coding: utf-8 -*-
"""
@File    :  THAIService.py
@Time    :  2022/3/31 19:29
@Author  :  changzheng
@Version :  1.0
@Desc    :  通号数据AI判断
"""
import pandas as pd
from Service.MalfunctionJudge.hs import time_xqqh
from Service.MalfunctionJudge.hs import time_comparision
from Service.MalfunctionJudge.hs import time_xiangguan
import csv
import re
import json
from Service.MalfunctionJudge.GetKRow import time_k_gap
from Service.MalfunctionJudge.DfOperation import get_column_data
from collections import Counter
from dateutil.parser import parse

class THAIService():
    def __init__(self):
        pass

    def SearchEigenvalues(self, example, mode):
        # 调用配置文件中的时间
        RBC_NoApplicationLayer_Time = "RBC_NoApplicationLayer_Time"
        DI_Before_Time = "DI_Before_Time"
        NR_Time = "NR_Time"
        AdjacentCellHandover_dptjchazhi = "AdjacentCellHandover_dptjchazhi"
        with open("config.json") as fp:
            json_data = json.load(fp)

        A_time_dire = []
        if mode == '通号':
            thtz = []
            THTZ = []
            th_Abis_rowNum = []  # Abis数据错误行数
            th_PRI_rowNum = []  # PRI数据错误行数
            th_A_rowNum = []  # PRI数据错误行数
            th_Abis_infodes = []
            th_PRI_infodes = []

            df = pd.read_excel(example)
            col = df.shape[1]  # 获取列数
            row = df.shape[0]  # 获取行数
            # 通号特征提取
            NBO = []
            # 邻区电平判断
            for i in range(row):
                abis_nbo_2 = str(df.loc[i, 'Abis_NB0_3'])
                if '-110' in abis_nbo_2:
                    NBO.append(i)
                    th_Abis_rowNum.append(i + 1)
                elif abis_nbo_2 is None and i != row - 1 and str(df.loc[i + 1, 'Abis_NB0_3']) is not None:
                    NBO.append(i)
                    th_Abis_rowNum.append(i + 1)
            if len(NBO) > 0:
                print('无邻区电平值，1')
                THTZ.append('无邻区电平值')
                th_Abis_infodes.append('无邻区电平值')
                thtz.append(1)
            else:
                print('存在邻区电平值，0')
                THTZ.append('存在邻区电平值')
                th_Abis_infodes.append('存在邻区电平值')
                thtz.append(0)

            # 小区切换失败
            link_invalid_data = []
            link_invalid_data_time = []
            crc = []
            crc_time = []
            SREJ = []
            a1 = 0
            a2 = 0
            for i in range(row):
                pri_explain_1 = str(df.loc[i, 'PRI_Explain_1'])
                pri_CRC_1 = str(df.loc[i, 'PRI_Check_1'])
                pri_SREJ_1 = str(df.loc[i, 'PRI_子类型_1'])
                if 'No Check' in pri_CRC_1:
                    crc.append(i)
                if 'Link Invalid Data' in pri_explain_1:
                    link_invalid_data.append(i)
                if 'SREJ' in pri_SREJ_1:
                    SREJ.append(i)
                    th_PRI_rowNum.append(i + 1)
            if len(link_invalid_data) >= 3:
                for i in range(3, len(link_invalid_data), 1):
                    if int(link_invalid_data[i]) - int(link_invalid_data[i - 1]) == 1 and int(
                            link_invalid_data[i - 1]) - int(
                        link_invalid_data[i - 2]) == 1 and int(link_invalid_data[i - 2]) - int(
                        link_invalid_data[i - 3]) == 1:
                        a1 = a1 + 1
                        link_invalid_data_time.append(str(df.loc[link_invalid_data[i] - 3, 'PRI_触发时间_1']))
                if a1 >= 1:
                    print('PRI持续出现Link Invalid Data消息，0')
                    b1 = 0
                    for i in range(len(link_invalid_data)):
                        th_PRI_rowNum.append(link_invalid_data[i] + 1)
                else:
                    print('无连续Link Invalid Data发生，1')
                    b1 = 1
            else:
                print('无连续Link Invalid Data发生，1')
                b1 = 1
            if len(crc) >= 3:
                for i in range(3, len(crc), 1):
                    if int(crc[i]) - int(crc[i - 1]) == 1 and int(crc[i - 1]) - int(crc[i - 2]) == 1 and int(
                            crc[i - 2]) - int(
                        crc[i - 3]) == 1:
                        a2 = a2 + 1
                        crc_time.append(str(df.loc[crc[i] - 3, 'PRI_触发时间_1']))
                if a2 >= 1:
                    print('PRI持续出现CRC校验失败，0')
                    b2 = 0
                    for i in range(len(crc)):
                        th_PRI_rowNum.append(crc[i] + 1)
                else:
                    print('无连续CRC校验失败发生，1')
                    b2 = 1
            else:
                print('无连续CRC校验失败发生，1')
                b2 = 1

            if len(SREJ) > 0:
                print('PRI接口存在SREJ帧')
                b3 = 0
            else:
                print('PRI接口不存在SREJ帧')
                b3 = 1

            if b1 == 0 or b2 == 0 or b3 == 0:
                THTZ.append('PRI持续出现CRC校验失败或连续Link Invalid Data或PRI接口存在SREJ帧')
                th_PRI_infodes.append('PRI持续出现CRC校验失败或连续Link Invalid Data或PRI接口存在SREJ帧')
                thtz.append(0)
            else:
                THTZ.append('无连续CRC校验失败或连续Link Invalid Data发生且PRI接口不存在SREJ帧')
                th_PRI_infodes.append('无连续CRC校验失败或连续Link Invalid Data发生且PRI接口不存在SREJ帧')
                thtz.append(1)

            disc = []
            disc_time = []
            handover1 = []
            time_command = []
            handover2 = []
            time_comolete = []
            handover3 = []
            time_failure = []
            handover4 = []
            time_chaoshi = []
            handover5 = []
            time_qhcs = []
            disc_um = []
            for i in range(row):
                abis_umlx_1 = str(df.loc[i, 'Abis_Um类型_1'])
                pri_zlx_1 = str(df.loc[i, 'PRI_子类型_1'])
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
                    time_comolete.append(str(df.loc[i, 'Abis_触发时间_1']))
                if 'HANDOVER FAILURE' in abis_umlx_1:
                    handover3.append(i)
                    time_failure.append(str(df.loc[i, 'Abis_触发时间_1']))
                    th_Abis_rowNum.append(i)
                if 'HANDOVER CONDITION INDICATION' in abis_umlx_1:
                    handover4.append(i)
                    time_chaoshi.append(str(df.loc[i, 'Abis_触发时间_1']))
                if 'CONNECTION FAILURE INDICATION' in abis_umlx_1:
                    handover5.append(i)
                    time_qhcs.append(str(df.loc[i, 'Abis_触发时间_1']))
            xx = []
            xy = []
            aa = []
            ab = []
            ac = []
            if len(handover1) <= len(handover2) and len(handover3) == 0:
                print('Abis信令中HANDOVER COMMAND与HANDOVER COMPLETE配对出现，0')  # 小区切换成功
                THTZ.append('Abis信令中HANDOVER COMMAND与HANDOVER COMPLETE配对出现')
                th_Abis_infodes.append('Abis信令中HANDOVER COMMAND与HANDOVER COMPLETE配对出现')
                thtz.append(0)
                for i in range(len(handover1)):
                    for j in range(len(handover2)):
                        if handover1[i] < handover2[j]:
                            xx.append(i)
                            xy.append(j)
                            break
            elif len(handover1) > len(handover2) and len(handover3) == 0:
                print('Abis信令只有HANDOVER COMMAND 无 HANDOVER COMPLETE，1')  # 小区切换失败
                THTZ.append('Abis信令只有HANDOVER COMMAND 无 HANDOVER COMPLETE')
                th_Abis_infodes.append('Abis信令只有HANDOVER COMMAND 无 HANDOVER COMPLETE')
                thtz.append(1)
                for i in range(len(handover1)):
                    th_Abis_rowNum.append(handover1[i] + 1)
                for i in range(len(handover2)):
                    th_Abis_rowNum.append(handover2[i] + 1)
                if len(disc_um) != 0:
                    THTZ.append(str(df.loc[disc_um[-1], 'Abis_关键字_1']))
            elif len(handover3) != 0:
                print('Abis信令中HANDOVER COMMAND与HANDOVER FAILURE配对出现，2')  # 小区切换失败
                THTZ.append('Abis信令中HANDOVER COMMAND与HANDOVER FAILURE配对出现')
                th_Abis_infodes.append('Abis信令中HANDOVER COMMAND与HANDOVER FAILURE配对出现')
                if len(handover3) > 1:
                    thtz.append(4)
                else:
                    thtz.append(2)
                if len(disc_um) != 0:
                    THTZ.append(str(df.loc[disc_um[-1], 'Abis_关键字_1']))
            elif len(handover4) > 3 and len(handover5) != 0:
                print('Abis信令中HANDOVER CONDITION INDICATION 和CONNECTION FAILURE INDICATION配对出现，3')  # 小区切换超时
                THTZ.append('Abis信令中HANDOVER CONDITION INDICATION 和CONNECTION FAILURE INDICATION配对出现')
                th_Abis_infodes.append('Abis信令中HANDOVER CONDITION INDICATION 和CONNECTION FAILURE INDICATION配对出现')
                thtz.append(3)
                for i in range(len(handover4)):
                    th_Abis_rowNum.append(handover4[i] + 1)
                for i in range(len(handover5)):
                    th_Abis_rowNum.append(handover5[i] + 1)
                if len(disc_um) != 0:
                    THTZ.append(str(df.loc[disc_um[-1], 'Abis_关键字_1']))

            if len(disc_time) != 0:
                for i in range(len(xx)):
                    a = time_xqqh(time_command[i], time_comolete[i], disc_time[-1])
                    aa.append(a)
                    for j in range(len(link_invalid_data_time)):
                        b = time_xqqh(time_command[i], time_comolete[i], link_invalid_data_time[j])
                        ab.append(b)
                    for k in range(len(crc_time)):
                        c = time_xqqh(time_command[i], time_comolete[i], crc_time[k])
                        ac.append(c)
            else:
                aa = []
                ab = []
                ac = []

            if 1 in aa:
                print('小区切换时发生故障，1')
                THTZ.append('Abis信令中HANDOVER COMMAND与HANDOVER FAILURE配对出现')
                th_Abis_infodes.append('Abis信令中HANDOVER COMMAND与HANDOVER FAILURE配对出现')
                thtz.append(1)
            elif 2 in aa:
                print('小区切换后发生故障，2')
                THTZ.append('小区切换后发生故障')
                th_PRI_infodes.append('小区切换后发生故障')
                thtz.append(2)
            else:
                if 1 in ab or 1 in ac:
                    print('小区切换时发生故障，1')
                    THTZ.append('小区切换时发生故障')
                    th_PRI_infodes.append('小区切换时发生故障')
                    thtz.append(1)
                elif 2 in ab or 2 in ac:
                    print('小区切换后发生故障，2')
                    THTZ.append('小区切换后发生故障')
                    th_PRI_infodes.append('小区切换后发生故障')
                    thtz.append(2)
                else:
                    print('发生故障与小区切换无关，0')
                    THTZ.append('发生故障与小区切换无关')
                    th_PRI_infodes.append('发生故障与小区切换无关')
                    thtz.append(0)
            # 上下行突发质量变差
            x = 4
            sxdpz = []
            sxtxzl = []
            xxdpz = []
            xxtxzl = []
            sxdpz_value = []
            sxdpz_value_num = 0
            a1 = 0
            a2 = 0
            a3 = 0
            a4 = 0
            for i in range(row):
                abis_sxdpz = str(df.loc[i, 'Abis_上行电平值_3']).replace(" ", "").replace("-", "").split('.')[0]
                abis_sxtxzl = str(df.loc[i, 'Abis_上行通信质量_3']).replace(" ", "").split('.')[0]
                abis_xxdpz = str(df.loc[i, 'Abis_下行电平值_3']).replace(" ", "").replace("-", "").split('.')[0]
                abis_xxtxzl = str(df.loc[i, 'Abis_下行通信质量_3']).replace(" ", "").split('.')[0]
                if abis_sxdpz.isdigit() is True:
                    sxdpz_value.append(abis_sxdpz)

                if abis_sxdpz.isdigit() is True:
                    if int(abis_sxdpz) >= 65:
                        sxdpz.append(i)

                if abis_xxdpz.isdigit() is True:
                    if int(abis_xxdpz) >= 65:
                        xxdpz.append(i)

                if abis_sxtxzl.isdigit() is True:
                    if int(abis_sxtxzl) > 3:
                        sxtxzl.append(i)

                if abis_xxtxzl.isdigit() is True:
                    if int(abis_xxtxzl) > 3:
                        xxtxzl.append(i)
            for i in range(2, len(sxdpz_value), 1):
                if int(sxdpz_value[i]) - int(sxdpz_value[i-2]) >= json_data[AdjacentCellHandover_dptjchazhi] + 10 and int(sxdpz_value[i]) >= 100:
                    sxdpz_value_temp = sxdpz_value[i:i+10]
                    sxdpz_value_temp_a = list(map(int, sxdpz_value_temp))
                    selected = [x for x in sxdpz_value_temp_a if x in range(100, 111)]
                    if len(selected) == 10:
                        sxdpz_value_num = sxdpz_value_num + 1

            for i in range(1, len(sxdpz), 1):
                if int(sxdpz[i]) - int(sxdpz[i - 1]) == 1:
                    a1 = a1 + 1
            for i in range(1, len(xxdpz), 1):
                if int(xxdpz[i]) - int(xxdpz[i - 1]) == 1:
                    a2 = a2 + 1
            for i in range(1, len(sxtxzl), 1):
                if int(sxtxzl[i]) - int(sxtxzl[i - 1]) == 1:
                    a3 = a3 + 1
            for i in range(1, len(xxtxzl), 1):
                if int(xxtxzl[i]) - int(xxtxzl[i - 1]) == 1:
                    a4 = a4 + 1
            if sxdpz_value_num > 0:
                print('上行电平值突降，2')
                THTZ.append('上行电平值突降')
                th_Abis_infodes.append('上行电平值突降')
                thtz.append(2)
            elif a1 >= x:
                print('上行电平值偏低，1')
                THTZ.append('上行电平值偏低')
                th_Abis_infodes.append('上行电平值偏低')
                thtz.append(1)
                for i in range(len(sxdpz)):
                    th_Abis_rowNum.append(sxdpz[i] + 1)
            else:
                print('上行电平值正常，0')
                THTZ.append('上行电平值正常')
                th_Abis_infodes.append('上行电平值正常')
                thtz.append(0)

            if a2 >= x:
                print('下行电平值偏低，1')
                THTZ.append('下行电平值偏低')
                th_Abis_infodes.append('下行电平值偏低')
                thtz.append(1)
                for i in range(len(xxdpz)):
                    th_Abis_rowNum.append(xxdpz[i] + 1)
            else:
                print('下行电平值正常，0')
                THTZ.append('下行电平值正常')
                th_Abis_infodes.append('下行电平值正常')
                thtz.append(0)

            if a3 >= x:
                print('上行通信质量连续异常，1')
                THTZ.append('上行通信质量连续异常')
                th_Abis_infodes.append('上行通信质量连续异常')
                thtz.append(1)
                for i in range(len(sxtxzl)):
                    th_Abis_rowNum.append(sxtxzl[i] + 1)
            else:
                print('上行通信质量正常，0')
                THTZ.append('上行通信质量正常')
                th_Abis_infodes.append('上行通信质量正常')
                thtz.append(0)

            if a4 >= x:
                print('下行通信质量连续异常，1')
                THTZ.append('下行通信质量连续异常')
                th_Abis_infodes.append('下行通信质量连续异常')
                thtz.append(1)
                for i in range(len(xxtxzl)):
                    th_Abis_rowNum.append(xxtxzl[i] + 1)
            else:
                print('下行通信质量正常，0')
                THTZ.append('下行通信质量正常')
                th_Abis_infodes.append('下行通信质量正常')
                thtz.append(0)

            # 提取连续异常的第一个值，判断时间是否在小区切换内。

            # V.110失步的三步判断
            v110 = []
            disc = []
            time_v110 = []
            dire_v110 = []
            time_disc = []
            a_xiangguan = []
            for i in range(row):
                pri_xxlx_1 = str(df.loc[i, 'PRI_消息类型_1'])
                pri_zlx_1 = str(df.loc[i, 'PRI_子类型_1'])
                pri_cfsj_1 = str(df.loc[i, 'PRI_触发时间_1'])
                if 'V.110帧失步' in pri_xxlx_1:  # 存在V.110失步
                    v110.append(i)
                    time_v110.append(pri_cfsj_1)
                    dire_v110.append(str(df.loc[i, 'PRI_Direction_1']))
                    th_PRI_rowNum.append(i + 1)
                if 'DISC' in pri_zlx_1 or 'DISCONNECT' in pri_zlx_1:  # 存在DISCONNECT
                    disc.append(i)
                    time_disc.append(pri_cfsj_1)
                    th_PRI_rowNum.append(i + 1)
            # 判断二者时间相关度
            if len(v110) != 0 and len(disc) != 0:
                for ii in range(len(time_v110)):
                    for ij in range(len(time_disc)):
                        a = time_xiangguan(time_v110[ii], time_disc[ij], 5)
                        if a == 1:
                            a_xiangguan.append(ii)
                            break
            if len(a_xiangguan) != 0:
                print('由于发生V.110失步导致DISCONNECT，1')
                THTZ.append('由于发生V.110失步导致DISCONNECT')
                th_PRI_infodes.append('由于发生V.110失步导致DISCONNECT')
                thtz.append(0)
                th_PRI_rowNum.append(i + 1)
            else:
                print('V.110失步与发生DISCONNECT无关，0')
                THTZ.append('V.110失步与发生DISCONNECT无关')
                th_PRI_infodes.append('V.110失步与发生DISCONNECT无关')
                thtz.append(0)
            if len(v110) != 0:
                for ik in range(len(v110)):
                    A_time_dire.append(
                        '发送第' + str(ik + 1) + '条V.110失步数据的触发时间：' + time_v110[ik] + '；触发方向：' + dire_v110[ik] + '\n')

            pri_xlfx = []
            time_xlfx = []
            j = 0
            for i in range(row):
                pri_xlfx_1 = str(df.loc[i, 'PRI_Direction_1'])
                pri_cfsj_1 = str(df.loc[i, 'PRI_触发时间_1'])
                if 'RBC->OBU' in pri_xlfx_1:
                    pri_xlfx.append(i)
                    time_xlfx.append(str(df.loc[i, 'PRI_触发时间_1']))
            if len(time_comolete) != 0:
                for j in range(len(time_xlfx)):
                    if time_comparision(time_comolete[-1], time_xlfx[j]) == 1:
                        break
                if len(pri_xlfx) == 0:
                    print('切换之后PRI接口只有RBC到车载方向的数据', 0)
                    THTZ.append('V.切换之后PRI接口只有RBC到车载方向的数据')
                    th_PRI_infodes.append('V.切换之后PRI接口只有RBC到车载方向的数据')
                    thtz.append(0)
                    th_PRI_rowNum.append(i + 1)
                elif len(pri_xlfx) != 0 and len(pri_xlfx) - j == pri_xlfx[-1] - pri_xlfx[j]:
                    print('切换之后PRI接口只有RBC到车载方向的数据', 0)
                    THTZ.append('切换之后PRI接口只有RBC到车载方向的数据')
                    th_PRI_infodes.append('切换之后PRI接口只有RBC到车载方向的数据')
                    thtz.append(0)
                    th_PRI_rowNum.append(i + 1)

                else:
                    print('切换之后PRI接口除了RBC到车载方向的数据还有其他方向数据', 1)
                    THTZ.append('切换之后PRI接口除了RBC到车载方向的数据还有其他方向数据')
                    th_PRI_infodes.append('切换之后PRI接口除了RBC到车载方向的数据还有其他方向数据')
                    thtz.append(1)
            else:
                j = len(time_xlfx)
                print('与切换无关', 2)
                THTZ.append('与切换无关')
                th_PRI_infodes.append('与切换无关')
                thtz.append(2)

            sabme_1_time = []
            sabme_1_time_num = []
            sabme_1_dire = []
            rr_1 = []
            for k in range(j, row):
                pri_sabme_1 = str(df.loc[k, 'PRI_子类型_1'])
                if 'SABME' in pri_sabme_1:
                    sabme_1_time.append(str(df.loc[k, 'PRI_触发时间_1']))
                    sabme_1_time_num.append(k)
                    sabme_1_dire.append(str(df.loc[k, 'PRI_Direction_1']))
                if 'RR' in pri_sabme_1:
                    rr_1.append(k)
            if len(sabme_1_time) >= 5 or len(rr_1) >= 5:
                print('发送五次SABME或五次RR,0')
                THTZ.append('发送五次SABME或五次RR')
                th_PRI_infodes.append('发送五次SABME或五次RR')
                thtz.append(0)
            else:
                print('未发送五次SABME或五次RR,1')
                THTZ.append('发送五次SABME或五次RR')
                th_PRI_infodes.append('发送五次SABME或五次RR')
                thtz.append(1)
                for i in range(len(sabme_1_time_num)):
                    th_PRI_rowNum.append(sabme_1_time_num[i] + 1)
            if len(sabme_1_time) != 0:
                for ik in range(len(sabme_1_time)):
                    A_time_dire.append(
                        '发送第' + str(ik + 1) + '条SABME数据的触发时间：' + sabme_1_time[ik] + '；触发方向：' + sabme_1_dire[ik] + '\n')

            # T1启动机制问题
            T1_e = 0
            subtype_list = get_column_data(df, "PRI_子类型_1")
            di_index_list = [i for i, x in enumerate(subtype_list) if x == "DISC"]
            if not di_index_list:
                T1_e = 1
                print('未发现探询帧延长发送,0')
                THTZ.append('未发现探询帧延长发送')
                th_PRI_infodes.append('未发现探询帧延长发送')
                thtz.append(0)
            for index in di_index_list:
                up_columns = time_k_gap(example, index, json_data[DI_Before_Time], mode)
                k_second_before_explain_data = get_column_data(df.iloc[index - up_columns:index + 1], "PRI_Explain_1")
                k_second_before_direction_data = get_column_data(df.iloc[index - up_columns:index + 1], "PRI_Direction_1")
                k_second_before_time_data = get_column_data(df.iloc[index - up_columns:index + 1], "PRI_触发时间_1")
                CBU_to_RBC_index_list = [i for i, x in enumerate(k_second_before_direction_data) if
                                         x == "OBU->RBC"]
                regex = r"N\(R\)=(\d*)"
                final_result = []
                time_data_all = []
                for index_CBU_to_RBC in CBU_to_RBC_index_list:
                    regex_str = k_second_before_explain_data[index_CBU_to_RBC]
                    regex_result = re.findall(regex, regex_str)
                    final_result += regex_result
                    if len(regex_result) != 0:
                        time_data_str = k_second_before_time_data[index_CBU_to_RBC]
                        time_data_all.append(time_data_str)
                if len(final_result) != 0:
                    NR_value_max = max(final_result, key=final_result.count)
                    NR_maxNum_time = []
                    for final_result_index in range(len(final_result)):
                        if final_result[final_result_index] == NR_value_max:
                            NR_maxNum_time.append(time_data_all[final_result_index])
                else:
                    NR_maxNum_time = []
                if len(NR_maxNum_time) != 0:
                    if (parse(NR_maxNum_time[-1]) - parse(NR_maxNum_time[0])).seconds > json_data[NR_Time]:
                        T1_e = 1
                        print('发现探询帧延长发送,1')
                        THTZ.append('发现探询帧延长发送')
                        th_PRI_infodes.append('发现探询帧延长发送')
                        thtz.append(1)
                        th_PRI_rowNum.append(index + 1)
                        break
            if T1_e == 0:
                print('未发现探询帧延长发送,0')
                THTZ.append('未发现探询帧延长发送')
                th_PRI_infodes.append('未发现探询帧延长发送')
                thtz.append(0)

            # RBC发送P42
            # P42_e = 0
            # subtype_list = get_column_data(df, "PRI_子类型_1")
            # layer_list = get_column_data(df, "PRI_Layer_1")
            # explain_list = get_column_data(df, "PRI_Explain_1")
            # di_index_list = [i for i, x in enumerate(subtype_list) if x == "DI"]
            # for index in di_index_list:
            #     application_index = 0
            #     for i in range(21):
            #         application_index = index - i
            #         if layer_list[application_index] == "Application":
            #             break
            #     if application_index == 0:
            #         continue
            #     regex_message = r"NID_MESSAGE = ([1-3]\d|\d),"
            #     regex_packet = r"NID_PACKET = (\d*)"
            #     NID_num_list = re.findall(regex_message, explain_list[application_index])
            #     if len(NID_num_list) > 0:
            #         packet_num_list = re.findall(regex_packet, explain_list[application_index])
            #         if len(packet_num_list) == 0:
            #             P42_e = 1
            #             print('发现RBC发送的24包长度异常,1')
            #             THTZ.append('发现RBC发送的24包长度异常')
            #             th_PRI_infodes.append('发现RBC发送的24包长度异常')
            #             thtz.append(1)
            #             break
            #         if packet_num_list[0] == "42":
            #             P42_e = 1
            #             print('发现RBC发送的24包内包含NID_PACKET=42,1')
            #             THTZ.append('发现RBC发送的24包内包含NID_PACKET=42')
            #             th_PRI_infodes.append('发现RBC发送的24包内包含NID_PACKET=42')
            #             thtz.append(1)
            #             th_PRI_rowNum.append(index + 1)
            #             break
            # if P42_e == 0:
            #     print('发现RBC发送的24包内包含NID_PACKET不为42,0')
            #     THTZ.append('发现RBC发送的24包内包含NID_PACKET不为42')
            #     th_PRI_infodes.append('发现RBC发送的24包内包含NID_PACKET不为42')
            #     thtz.append(0)

            # GSM-R网络和车载数据未发现异常，RBC断开连接与ISDN服务器
            rbc_c = 0
            gap = 10
            subtype_list = get_column_data(df, "PRI_子类型_1")
            message_list = get_column_data(df, "PRI_消息类型_1")
            time_list = get_column_data(df, "PRI_触发时间_1")
            explain_list = get_column_data(df, "PRI_Explain_1")
            di_index_list = [i for i, x in enumerate(subtype_list) if x.upper() == "DISCONNECT"]
            for index in di_index_list:
                message_index = 0
                for i in range(21):
                    message_index = index - i
                    if message_list[message_index] == "APDU":
                        break
                if message_index == 0:
                    continue
                if explain_list[message_index][0:9] == "RBC-->ATP":
                    time_gap = int(time_list[message_index][-6:-4]) - int(time_list[index][-6:-4])
                    if time_gap > gap:
                        rbc_c = 1
                        print('正常交互的RBC最后一条消息与disc相差10秒以内,1')
                        THTZ.append('正常交互的RBC最后一条消息与disc相差10秒')
                        th_PRI_infodes.append('正常交互的RBC最后一条消息与disc相差10秒')
                        thtz.append(1)
                        th_PRI_rowNum.append(index + 1)
                        break
                    if time_gap < 0 and 0 - time_gap > gap:
                        rbc_c = 1
                        print('正常交互的RBC最后一条消息与disc相差10秒以内,1')
                        THTZ.append('正常交互的RBC最后一条消息与disc相差10秒')
                        th_PRI_infodes.append('正常交互的RBC最后一条消息与disc相差10秒')
                        thtz.append(1)
                        break
            if rbc_c == 0:
                print('正常交互的RBC最后一条消息与disc相差10秒以上,0')
                THTZ.append('正常交互的RBC最后一条消息与disc相差10秒以上')
                th_PRI_infodes.append('正常交互的RBC最后一条消息与disc相差10秒以上')
                thtz.append(0)

            # 应用层不发送消息导致20秒内未收到回复断开
            apdu = 0
            subtype_list = get_column_data(df, "PRI_子类型_1")
            message_type_list = get_column_data(df, "PRI_消息类型_1")

            di_index_list = [i for i, x in enumerate(subtype_list) if x == "DI"]
            if not di_index_list:
                apdu = 1
                print('应用层20秒内收到回复ATP不发送DI,0')
                THTZ.append('应用层20秒内收到回复ATP不发送DI')
                th_PRI_infodes.append('应用层20秒内收到回复ATP不发送DI')
                thtz.append(0)
            for index in di_index_list:
                RBCtoOBC_message_num = 0
                if message_type_list[index] == "SAPDU":
                    # iloc 经典左闭右开
                    # 获取行数的函数
                    up_columns = time_k_gap(example, index, json_data[DI_Before_Time], mode)
                    k_second_before_message_type_data = get_column_data(df.iloc[index - up_columns:index + 1], "PRI_消息类型_1")
                    k_second_before_explain_data = get_column_data(df.iloc[index - up_columns:index + 1], "PRI_Explain_1")
                    regex1 = r"NID_MESSAGE = (\d*)"
                    for messageType_index in range(len(k_second_before_message_type_data)-1, -1, -1):
                        if k_second_before_message_type_data[messageType_index] == "APDU":
                            regex_str = k_second_before_explain_data[messageType_index]
                            regex_result = re.findall(regex1, regex_str)
                            if int(regex_result[0]) < 100:
                                RBCtoOBC_message_num = RBCtoOBC_message_num + 1
                                RBCtoOBC_time = str(df.loc[index - len(k_second_before_message_type_data) + messageType_index, "PRI_触发时间_1"]).replace('. ', '.')
                                SaPDUDI_time = str(df.loc[index, "PRI_触发时间_1"]).replace('. ', '.')
                                if RBCtoOBC_message_num == 2:
                                    break
                                if (parse(SaPDUDI_time) - parse(RBCtoOBC_time)).seconds >= json_data[RBC_NoApplicationLayer_Time]:
                                    apdu = 1
                                    print('应用层20秒内未收到回复ATP发送DI,1')
                                    THTZ.append('应用层20秒内未收到回复ATP发送DI')
                                    th_PRI_infodes.append('应用层20秒内未收到回复ATP发送DI')
                                    th_PRI_rowNum.append(index + 1)
                                    thtz.append(1)
                                    break
                    if apdu == 1:
                        break
            if apdu == 0:
                print('应用层20秒内收到回复ATP不发送DI,0')
                THTZ.append('应用层20秒内收到回复ATP不发送DI')
                th_PRI_infodes.append('应用层20秒内未收到回复ATP发送DI')
                thtz.append(0)



            return thtz, THTZ, A_time_dire, th_PRI_rowNum, th_Abis_rowNum, th_PRI_infodes, th_Abis_infodes

# THAIService().SearchEigenvalues(r"C:\Users\zhu\Documents\GitHub\CTCS\GUI\data\excel_time\G1273-202008055807-Data+_to_.xls",'通号')