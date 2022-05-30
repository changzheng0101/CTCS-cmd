# -*- coding: utf-8 -*-
"""
@File    :  IntegrateExcel.py
@Time    :  2022/4/3 10:05
@Author  :  zhuyugang
@Version :  1.0
@Desc    :  整合所有Excel表
"""

import os

import xlrd
import xlwt


class IntegrateExcel(object):
    def __init__(self, my_model, file_name):
        self.my_model = my_model
        self.file_name = file_name

        self.unzippath = "./data/unzip"  # unzip path
        self.downloadpath = "./data/download"  # download path
        filepath = self.unzippath + '/' + file_name
        self.sample_list = []

        file_list_temp = self.listdir(filepath)
        self.file_list = self.splitlist(file_list_temp)

    # def integrate_run(self, my_model, file_name):
    #     # download
    #     print("***********************************下载***********************************")
    #
    #     unzippath = "./data/unzip"  # unzip path
    #     downloadpath = "./data/download"  # download path
    #     filepath = unzippath + '/' + file_name
    #     sample_list = []
    #
    #     file_list_temp = self.listdir(filepath)
    #     file_list = self.splitlist(file_list_temp)
    #
    #     mysheet = ExcelManage(unzippath, file_list, sample_list, downloadpath, '', '', '')
    #     mysheet.main_integrate(my_model, file_name)
    #     return True

    def main_integrate(self):
        if not os.path.exists(self.downloadpath):
            os.makedirs(self.downloadpath)

        self.excel_outputpath = self.downloadpath + '/' + self.file_name+'.xls'
        self.integration2excel()
        print("[+] create excel: " + self.excel_outputpath)
        return True

    def integration2excel(self):
        # 初始化第一行的style
        style_firstrow = xlwt.XFStyle()
        # 边框
        borders = xlwt.Borders()
        borders.left = 1
        borders.right = 1
        borders.top = 1
        borders.bottom = 1
        # borders.bottom_colour = 0x3A
        style_firstrow.borders = borders
        # 字体
        font = xlwt.Font()
        font.name = u'宋体'
        font.bold = False
        font.height = 20 * 12
        font.colour_index = 0
        style_firstrow.font = font
        # 背景
        badBG = xlwt.Pattern()
        badBG.pattern = badBG.SOLID_PATTERN
        badBG.pattern_fore_colour = 22  # https://blog.csdn.net/u012950066/article/details/100009290
        style_firstrow.pattern = badBG

        # 初始化其他行的style
        self.style_dataRow = xlwt.XFStyle()
        font = xlwt.Font()
        font.name = u'宋体'
        font.bold = False
        font.height = 20 * 12
        self.style_dataRow.font = font
        self.style_dataRow.borders = borders
        # 采用标准的数据格式，创建excel
        if self.my_model == '通号':
            fields = ['PRI_序号_1',
                      'PRI_触发时间_1', 'PRI_MSISDN/IMSI_1', 'PRI_RBC号码_1', 'PRI_机车号_1', 'PRI_车次号_1',
                      'PRI_线路_1', 'PRI_Layer_1', 'PRI_消息类型_1', 'PRI_子类型_1', 'PRI_设备号_1', 'PRI_端口号_1',
                      'PRI_时隙号_1', 'PRI_Status_1', 'PRI_原因值_1', 'PRI_Check_1', 'PRI_Direction_1', 'PRI_CTCS_ID_1',
                      'PRI_RBCID_1', 'PRI_T_Train_1', 'PRI_LRBG_1', 'PRI_公里标_1', 'PRI_当前速度_1', 'PRI_Length_1',
                      'PRI_Data_1', 'PRI_Explain_1',
                      'PRI_序号_2',
                      'PRI_发生时间_2', 'PRI_车次号_2', 'PRI_机车号_2', 'PRI_MSISDN/IMSI_2', 'PRI_RBC号码_2',
                      'PRI_CTCS_ID_2', 'PRI_故障类型_2', 'PRI_线路_2', 'PRI_发生公里标_2', 'PRI_速度(km/h)_2',
                      'PRI_是否恢复_2', 'PRI_恢复时间_2', 'PRI_恢复公里标_2', 'PRI_是否确认_2',
                      'Abis_序号_1',
                      'Abis_触发时间_1', 'Abis_MSISDN/IMSI_1', 'Abis_机车号_1', 'Abis_车次号_1', 'Abis_线路_1',
                      'Abis_消息类型_1', 'Abis_Um类型_1', 'Abis_关键字_1', 'Abis_方向_1', 'Abis_正反环_1', 'Abis_所属BSC_1',
                      'Abis_小区_1', 'Abis_LAC_1', 'Abis_CI_1', 'Abis_ARFCN_1', 'Abis_公里标_1', 'Abis_当前速度(km/h)_1',
                      'Abis_长度_1', 'Abis_数据内容_1',
                      'Abis_序号_2',
                      'Abis_机车号_2', 'Abis_车次号_2', 'Abis_线路_2', 'Abis_MSISDN/IMSI_2', 'Abis_公里标_2',
                      'Abis_速度(km/h)_2', 'Abis_切换前BCCH_2', 'Abis_切换后BCCH_2', 'Abis_切换前小区_3',
                      'Abis_切换后小区_3', 'Abis_切换前TCH_2', 'Abis_切换后TCH_2', 'Abis_切换发起时间_2',
                      'Abis_切换结束时间_2', 'Abis_切换执行时间(ms)_2', 'Abis_切换结果_2', 'Abis_切换失败原因_2',
                      'Abis_非规则切换_2', 'Abis_切换前LAC_2', 'Abis_切换前CI_2', 'Abis_切换后LAC_2', 'Abis_切换后CI_2',
                      'Abis_序号_3',
                      'Abis_触发时间_3', 'Abis_MSISDN/IMSI_3', 'Abis_机车号_3', 'Abis_车次号_3', 'Abis_线路_3',
                      'Abis_小区_3', 'Abis_LAC_3', 'Abis_CI_3', 'Abis_公里标_3', 'Abis_当前速度(km/h)_3', 'Abis_ARFCN_3',
                      'Abis_MR序号_3', 'Abis_上行电平值_3', 'Abis_上行通信质量_3', 'Abis_下行电平值_3',
                      'Abis_下行通信质量_3', 'Abis_TA值_3', 'Abis_NB0_3', 'Abis_BCCH999_3', 'Abis_BCCH1000_3',
                      'Abis_BCCH1001_3', 'Abis_BCCH1002_3', 'Abis_BCCH1003_3', 'Abis_BCCH1004_3', 'Abis_BCCH1005_3',
                      'Abis_BCCH1006_3', 'Abis_BCCH1007_3', 'Abis_BCCH1008_3', 'Abis_BCCH1009_3', 'Abis_BCCH1010_3',
                      'Abis_BCCH1011_3', 'Abis_BCCH1012_3', 'Abis_BCCH1013_3', 'Abis_BCCH1014_3', 'Abis_BCCH1015_3',
                      'Abis_BCCH1016_3', 'Abis_BCCH1017_3', 'Abis_BCCH1018_3', 'Abis_BCCH1019_3', 'Abis_数据内容_3',
                      'Abis_标志位_3', 'Abis_显示时间_3',
                      'A_序号_1',
                      'A_触发时间_1', 'A_MSISDN/IMSI_1', 'A_机车号_1', 'A_车次号_1', 'A_线路_1', 'A_消息类型_1',
                      'A_BSSMAP类型_1', 'A_SCCP类型_1', 'A_Um类型_1', 'A_原因值_1', 'A_原因值2_1', 'A_方向_1', 'A_设备号_1',
                      'A_端口号_1', 'A_时隙号_1', 'A_所属BSC_1', 'A_小区_1', 'A_LAC_1', 'A_CI_1', 'A_公里标_1',
                      'A_当前速度(km/h)_1', 'A_长度_1', 'A_数据内容_1', 'A_序号_2', 'A_机车号_2', 'A_车次号_2', 'A_线路_2',
                      'A_MSISDN/IMSI_2', 'A_公里标_2', 'A_速度(km/h)_2', 'A_切换前小区_2', 'A_切换后小区_2',
                      'A_切换前BSC_2', 'A_切换后BSC_2', 'A_切换发起时间_2', 'A_切换结束时间_2', 'A_切换结果_2',
                      'A_触发切换原因值_2', 'A_切换失败原因_2', 'A_无线资源原因值_2', 'A_非规则切换_2', 'A_切换前LAC_2',
                      'A_切换前CI_2', 'A_切换后LAC_2', 'A_切换后CI_2', 'ATPCU_LOG']
        elif self.my_model == '铁科':
            fields = ['PRI_序号_1',
                      'PRI_触发时间_1', 'PRI_移动台MSISDN_1', 'PRI_RBC号码_1', 'PRI_机车号_1', 'PRI_车次号_1', 'PRI_数据传送方向_1',
                      'PRI_CTCS_ID_1', 'PRI_LRBG_1', 'PRI_公里标_1', 'PRI_速度_1', 'PRI_信令类型_1', 'PRI_子类型_1', 'PRI_--_1',
                      'PRI_OBC->RBC_1', 'PRI_RBC->OBC_1', 'PRI_CRC校验_1', 'PRI_T_TRAIN_1', 'PRI_设备号_1', 'PRI_端口号_1',
                      'PRI_时隙号_1', 'PRI_数据长度_1', 'PRI_数据内容_1',
                      'Abis_序号_1',
                      'Abis_触发时间_1', 'Abis_MSISDN_1', 'Abis_用户识别码_1', 'Abis_线路_1', 'Abis_小区名称_1', 'Abis_信令类型_1',
                      'Abis_Um类型_1', 'Abis_方向_1', 'Abis_正反环_1', 'Abis_BCCH_1', 'Abis_公里标_1', 'Abis_速度_1', 'Abis_关键字_1',
                      'Abis_端口号_1', 'Abis_时隙号_1', 'Abis_数据长度_1', 'Abis_数据内容_1',
                      'Abis_序号_2',
                      'Abis_触发时间_2', 'Abis_MSISDN_2', 'Abis_用户识别码_2', 'Abis_小区_2', 'Abis_公里标_2', 'Abis_速度_2',
                      'Abis_BCCH_2',
                      'Abis_RxLevelUp_2', 'Abis_RxLevelDown_2', 'Abis_RxQualUp_2', 'Abis_RxQualDown_2', 'Abis_TA_2',
                      'Abis_NB0电平_2', 'Abis_标记位_2',
                      'Abis_频点999_2', 'Abis_频点1000_2', 'Abis_频点1001_2', 'Abis_频点1002_2', 'Abis_频点1003_2',
                      'Abis_频点1004_2', 'Abis_频点1005_2',
                      'Abis_频点1006_2', 'Abis_频点1007_2', 'Abis_频点1008_2', 'Abis_频点1009_2', 'Abis_频点1010_2',
                      'Abis_频点1011_2', 'Abis_频点1012_2',
                      'Abis_频点1013_2', 'Abis_频点1014_2', 'Abis_频点1015_2', 'Abis_频点1016_2', 'Abis_频点1017_2',
                      'Abis_频点1018_2', 'Abis_频点1019_2', 'Abis_时间_2',
                      'Abis_序号_3',
                      'Abis_MSISDN_3', 'Abis_用户识别码_3', 'Abis_公里标_3', 'Abis_速度_3', 'Abis_切换前BCCH_3', 'Abis_切换后BCCH_3',
                      'Abis_切换前小区_3',
                      'Abis_切换后小区_3', 'Abis_切换前TCH_3', 'Abis_切换后TCH_3', 'Abis_切换发起时间_3', 'Abis_切换结束时间_3',
                      'Abis_切换执行时间_3',
                      'Abis_切换结果_3', 'Abis_切换原因_3', 'Abis_切换失败原因_3',
                      'A_机车号_1',
                      'A_MSISDN_1', 'A_用户识别码_1', 'A_触发时间_1', 'A_LAC_1', 'A_CI_1', 'A_小区名称_1', 'A_SCCP类型_1',
                      'A_BSSMAP/DTAP_1', 'A_BSSMAP类型_1', 'A_Um类型_1', 'A_方向_1', 'A_公里标_1', 'A_原因值_1', 'A_关键字_1',
                      'A_切换开始时间_2',
                      'A_切换结束时间_2', 'A_切换状态_2', 'A_LAC(源)_2', 'A_CI(源)_2', 'A_切换前小区名称_2', 'A_LAC(目的)_2',
                      'A_CI(目的)_2', 'A_切换后小区名称_2', 'A_切换是否成功_2', 'A_切换原因_2', 'A_切换失败原因_2', 'A_公里标_2']
        else:
            fields = ['PRI_序号_1',
                      'PRI_触发时间_1', 'PRI_信令方向_1', 'PRI_车次号_1', 'PRI_小区信息_1', 'PRI_CRC校验_1', 'PRI_帧类型_1',
                      'PRI_链路层_1', 'PRI_传输层_1', 'PRI_安全层_1', 'PRI_应用层_1', 'PRI_发送方T_Train_1', 'PRI_确认对方T_Train_1',
                      'PRI_车->地_1',
                      'PRI_地->车_1', 'PRI_探询帧标识_1', 'PRI_CTCS_ID_1', 'PRI_MSISDN_1', 'PRI_IMSI_1', 'PRI_LAC/CI_1',
                      'PRI_公里标_1',
                      'PRI_拆链原因_1', 'PRI_速度(km/h)_1', 'PRI_机车号_1', 'PRI_RBC号码_1', 'PRI_台账名称_1', 'PRI_应答器信息_1',
                      'PRI_数据来源路局_1',
                      'PRI_列控等级_1', 'PRI_采集卡_1', 'PRI_端口_1', 'PRI_时隙_1', 'PRI_信令长度_1', 'PRI_信令内容_1',
                      'PRI_序号_2',
                      'PRI_IMSI_2', 'PRI_主叫MSISDN_2', 'PRI_机车号_2', 'PRI_车次号_2', 'PRI_CTCS_ID_2', 'PRI_RBC号码_2',
                      'PRI_台账名称_2', 'PRI_起呼时间_2', 'PRI_挂机时间_2', 'PRI_业务时长_2', 'PRI_呼叫类型_2', 'PRI_拆链原因_2', 'PRI_话单结果_2',
                      'PRI_是否本局起呼_2', 'PRI_拆链发起方_2', 'PRI_起呼小区_2', 'PRI_拆链小区_2', 'PRI_起呼LAC/CI_2',
                      'PRI_拆链LAC/CI_2', 'PRI_起呼公里标_2', 'PRI_拆链公里标_2', 'PRI_拆链速度(km/h)_2',
                      'Abis_序号_1',
                      'Abis_记录时间_1', 'Abis_信令方向_1', 'Abis_IMSI_1', 'Abis_MSISDN_1', 'Abis_Um类型_1', 'Abis_机车号_1',
                      'Abis_车次号_1',
                      'Abis_小区信息_1', 'Abis_CTCS_ID_1', 'Abis_LAC/CI_1', 'Abis_公里标_1', 'Abis_信令类型_1', 'Abis_切换原因_1',
                      'Abis_拆链原因_1', 'Abis_无线资源原因值_1', 'Abis_原因值_1', 'Abis_速度(km/h)_1', 'Abis_TCH_1', 'Abis_信道时隙_1',
                      'Abis_TA_1',
                      'Abis_RBC号码_1', 'Abis_台账名称_1', 'Abis_数据来源路局_1', 'Abis_采集卡_1', 'Abis_端口_1', 'Abis_时隙_1',
                      'Abis_正反环_1', 'Abis_信令长度_1', 'Abis_信令内容_1',
                      'Abis_序号_2',
                      'Abis_日期_2', 'Abis_触发时间_2', 'Abis_小区信息_2', 'Abis_公里标_2', 'Abis_速度(km/h)_2', 'Abis_BCCH_2',
                      'Abis_TCH_2', 'Abis_测量报告类型_2', 'Abis_测量报告序号_2', 'Abis_上行接收电平(dBm)_2', 'Abis_上行接收质量_2',
                      'Abis_下行接收电平(dBm)_2',
                      'Abis_下行接收质量_2', 'Abis_TA_2', 'Abis_NB0Arfcn_2', 'Abis_NB0Level(dBm)_2', 'Abis_1000_2',
                      'Abis_1001_2', 'Abis_1005_2',
                      'Abis_1006_2', 'Abis_1012_2', 'Abis_1013_2', 'Abis_1015_2', 'Abis_IMSI_2', 'Abis_MSISDN_2',
                      'Abis_机车号_2', 'Abis_车次号_2',
                      'Abis_CTCS_ID_2', 'Abis_采集卡_2', 'Abis_端口_2', 'Abis_时隙_2', 'Abis_切换标识_2', 'Abis_TA切换标识_2',
                      'Abis_序号_3',
                      'Abis_开始时间_3', 'Abis_结束时间_3', 'Abis_执行时间_3', 'Abis_IMSI_3', 'Abis_车次号_3', 'Abis_机车号_3',
                      'Abis_切换前小区_3',
                      'Abis_切换后小区_3', 'Abis_切换结果_3', 'Abis_切换原因_3', 'Abis_切换失败原因_3', 'Abis_RBC号码_3', 'Abis_台账名称_3',
                      'Abis_MSISDN_3', 'Abis_CTCS_ID_3',
                      'Abis_公里标_3', 'Abis_速度(km/h)_3', 'Abis_切换前LAC/CI_3', 'Abis_切换后LAC/CI_3', 'Abis_切换前BCCH_3',
                      'Abis_切换后BCCH_3', 'Abis_切换前TCH_3', 'Abis_切换后TCH_3',
                      'Abis_切换前上行电平_3', 'Abis_切换前上行质量_3', 'Abis_切换前下行电平_3', 'Abis_切换前下行质量_3', 'Abis_切换后下行电平_3',
                      'Abis_切换前TA_3', 'Abis_切换后TA_3',
                      'Abis_序号_4',
                      'Abis_IMSI_4', 'Abis_主叫MSISDN_4', 'Abis_机车号_4', 'Abis_车次号_4', 'Abis_CTCS_ID_4', 'Abis_RBC号码_4',
                      'Abis_台账名称_4', 'Abis_起呼时间_4',
                      'Abis_拆链时间_4', 'Abis_业务时长_4', 'Abis_呼叫类型_4', 'Abis_起呼小区_4', 'Abis_拆链小区_4', 'Abis_起呼LAC/CI_4',
                      'Abis_拆链LAC/CI_4', 'Abis_起呼载频_4',
                      'Abis_拆链载频_4', 'Abis_拆链发起方_4', 'Abis_拆链原因_4', 'Abis_起呼公里标_4', 'Abis_拆链公里标_4', 'Abis_拆链速度(km/h)_4',
                      'A_序号_1',
                      'A_记录时间_1', 'A_信令方向_1', 'A_IMSI_1', 'A_MSISDN_1', 'A_机车号_1', 'A_车次号_1', 'A_CTCS_ID_1', 'A_小区信息_1',
                      'A_LAC/CI_1',
                      'A_公里标_1', 'A_SCCP信令类型_1', 'A_BSSMAP_DTAP_IND_1', 'A_BSSMAP信息类型_1', 'A_Um信息类型_1', 'A_原因值_1',
                      'A_无线资源原因值_1', 'A_速度(km/h)_1', 'A_RBC号码_1',
                      'A_台账名称_1', 'A_BSC_1', 'A_CIC_1', 'A_应答器编号_1', 'A_数据来源路局_1', 'A_采集卡_1', 'A_端口_1', 'A_时隙_1',
                      'A_信令长度_1', 'A_信令内容_1',
                      'A_序号_2', 'A_开始时间_2', 'A_结束时间_2', 'A_执行时间_2', 'A_IMSI_2', 'A_MSISDN_2', 'A_机车号_2', 'A_车次号_2',
                      'A_CTCS_ID_2', 'A_RBC号码_2',
                      'A_台账名称_2', 'A_切换原因_2', 'A_切换结果_2', 'A_切换失败原因_2', 'A_无线资源原因_2', 'A_公里标_2', 'A_速度(km/h)_2',
                      'A_切换前采集设备_2', 'A_切换后采集设备_2',
                      'A_切换前BSC_2', 'A_切换后BSC_2', 'A_切换前小区_2', 'A_切换目标小区_2', 'A_切换前LAC/CI_2', 'A_切换后LAC/CI_2',
                      'A_切换前BCCH_2', 'A_切换后BCCH_2', 'A_切换前CIC_2', 'A_切换后CIC_2',
                      'A_序号_3',
                      'A_IMSI_3', 'A_主叫MSISDN_3', 'A_机车号_3', 'A_车次号_3', 'A_CTCS_ID_3', 'A_RBC号码_3', 'A_台账名称_3',
                      'A_起呼时间_3', 'A_拆链时间_3', 'A_业务时长_3',
                      'A_起呼小区_3', 'A_拆链小区_3', 'A_起呼LAC/CI_3', 'A_拆链LAC/CI_3', 'A_呼叫类型_3', 'A_拆链发起方_3', 'A_起呼公里标_3',
                      'A_拆链公里标_3', 'A_拆链速度(km/h)_3', 'A_拆链原因清除原因_3']

        workbook = xlwt.Workbook()
        sheet = workbook.add_sheet('sheet1', cell_overwrite_ok=True)

        # 设置冻结窗口
        sheet.set_panes_frozen('1')  # 设置冻结为
        sheet.set_horz_split_pos(1)  # 设置冻结行数

        # 写入字段信息
        for field in range(0, len(fields)):
            sheet.write(0, field, fields[field], style_firstrow)
            sheet.col(field).width = 256 * 20  # 设置宽度

        # 写入数据信息
        excel_list = []
        for file_addr in self.file_list:
            if os.path.splitext(file_addr)[1] == '.xls' or os.path.splitext(file_addr)[1] == '.xlsx':
                excel_list.append(file_addr)

        row_pri1, row_pri2, row_abis1, row_abis2, row_abis3, row_abis4, row_a1, row_a2, row_a3 = 1, 1, 1, 1, 1, 1, 1, 1, 1
        row_ini, col_ini, col_offset, wrow = 1, 1, 1, 1
        row_tmp = 0

        if self.my_model == '通号':
            for file_addr in excel_list:  # 获取list里所有文件名
                excel_data = xlrd.open_workbook(file_addr, encoding_override='utf-8')  # 打开excel文件
                if 'PRI' in file_addr:  # 找PRI接口数据
                    for sheet_num in range(0, len(excel_data.sheet_names())):
                        if sheet_num == 0:
                            col_offset = -1
                            wrow = row_pri1
                        else:
                            col_offset = 25
                            wrow = row_pri2
                        row_tmp = self.write_excel(sheet, file_addr, excel_data, sheet_num, row_ini, col_ini, col_offset,
                                              wrow)  # 调用写好的写入函数
                        if sheet_num == 0:
                            row_pri1 = row_tmp
                        else:
                            row_pri2 = row_tmp
                elif 'Abis' in file_addr:
                    for sheet_num in range(0, len(excel_data.sheet_names())):
                        if sheet_num == 0:
                            col_offset = 40
                            wrow = row_abis1
                        elif sheet_num == 1:
                            col_offset = 60
                            wrow = row_abis2
                        else:
                            col_offset = 83
                            wrow = row_abis3
                        row_tmp = self.write_excel(sheet, file_addr, excel_data, sheet_num, row_ini, col_ini, col_offset,
                                              wrow)
                        if sheet_num == 0:
                            row_abis1 = row_tmp
                        elif sheet_num == 1:
                            row_abis2 = row_tmp
                        else:
                            row_abis3 = row_tmp
                else:
                    for sheet_num in range(0, len(excel_data.sheet_names())):
                        if sheet_num == 0:
                            col_offset = 126
                            wrow = row_a1
                        else:
                            col_offset = 150
                            wrow = row_a2
                        row_tmp = self.write_excel(sheet, file_addr, excel_data, sheet_num, row_ini, col_ini, col_offset,
                                              wrow)
                        if sheet_num == 0:
                            row_a1 = row_tmp
                        else:
                            row_a2 = row_tmp
        elif self.my_model == '铁科':  # 与通号基本类似
            col_ini = 0
            for file_addr in excel_list:
                # 排除其他数据，留下三接口数据
                if '呼叫' in file_addr or 'Um' in file_addr or 'Igsm-r' in file_addr:
                    continue
                elif 'PRI' in file_addr or 'Abis' in file_addr or 'A' in file_addr:
                    pass
                else:
                    continue

                excel_data = xlrd.open_workbook(file_addr)  # 打开excel文件
                if 'PRI' in file_addr:
                    for sheet_num in range(0, len(excel_data.sheet_names())):
                        col_offset = 0
                        wrow = row_pri1
                        row_tmp = self.write_excel(sheet, file_addr, excel_data, sheet_num, row_ini, col_ini, col_offset,
                                              wrow)
                        row_pri1 = row_tmp
                elif 'Abis' in file_addr:
                    for sheet_num in range(0, len(excel_data.sheet_names())):
                        if sheet_num == 0:
                            col_offset = 41
                            wrow = row_abis1
                        elif sheet_num == 1:
                            col_offset = 78
                            wrow = row_abis2
                        else:
                            col_offset = 23
                            wrow = row_abis3
                        row_tmp = self.write_excel(sheet, file_addr, excel_data, sheet_num, row_ini, col_ini, col_offset,
                                              wrow)
                        if sheet_num == 0:
                            row_abis1 = row_tmp
                        elif sheet_num == 1:
                            row_abis2 = row_tmp
                        else:
                            row_abis3 = row_tmp
                else:
                    for sheet_num in range(0, len(excel_data.sheet_names())):
                        if sheet_num == 0:
                            col_offset = 95
                            wrow = row_a1
                        else:
                            col_offset = 110
                            wrow = row_a2
                        row_tmp = self.write_excel(sheet, file_addr, excel_data, sheet_num, row_ini, col_ini, col_offset,
                                              wrow)
                        if sheet_num == 0:
                            row_a1 = row_tmp
                        else:
                            row_a2 = row_tmp

        else:  # 交大数据，与通号基本类似
            row_ini = 1
            col_ini = 0
            for file_addr in excel_list:
                # print(file_addr.find('详细信息'))
                if '详细信息' in file_addr:
                    excel_data = xlrd.open_workbook(file_addr, encoding_override='utf-8')  # 打开excel文件
                    if 'PRI' in file_addr:  # 找PRI接口数据

                        for sheet_num in range(0, len(excel_data.sheet_names())):
                            if sheet_num == 0:
                                col_offset = -1
                                wrow = row_pri1
                            else:
                                col_offset = 25
                                wrow = row_pri2
                            row_tmp = self.write_excel(sheet, file_addr, excel_data, sheet_num, row_ini, col_ini, col_offset,
                                                  wrow)  # 调用写好的写入函数
                            if sheet_num == 0:
                                row_pri1 = row_tmp
                            else:
                                row_pri2 = row_tmp
                    elif 'Abis' in file_addr:
                        for sheet_num in range(0, len(excel_data.sheet_names())):
                            if sheet_num == 0:
                                col_offset = 40
                                wrow = row_abis1
                            elif sheet_num == 1:
                                col_offset = 60
                                wrow = row_abis2
                            else:
                                col_offset = 83
                                wrow = row_abis3
                            row_tmp = self.write_excel(sheet, file_addr, excel_data, sheet_num, row_ini, col_ini, col_offset,
                                                  wrow)
                            if sheet_num == 0:
                                row_abis1 = row_tmp
                            elif sheet_num == 1:
                                row_abis2 = row_tmp
                            else:
                                row_abis3 = row_tmp
                    else:
                        for sheet_num in range(0, len(excel_data.sheet_names())):
                            if sheet_num == 0:
                                col_offset = 126
                                wrow = row_a1
                            else:
                                col_offset = 150
                                wrow = row_a2
                            row_tmp = self.write_excel(sheet, file_addr, excel_data, sheet_num, row_ini, col_ini, col_offset,
                                                  wrow)
                            if sheet_num == 0:
                                row_a1 = row_tmp
                            else:
                                row_a2 = row_tmp
                else:
                    excel_data = xlrd.open_workbook(file_addr)  # 打开excel文件
                    if 'PRI' in file_addr:
                        if '信令' in file_addr:
                            col_offset = 0
                            wrow = row_pri1
                        else:
                            col_offset = 34
                            wrow = row_pri2
                        row_tmp = self.write_excel(sheet, file_addr, excel_data, 0, row_ini, col_ini, col_offset, wrow)
                        if '信令' in file_addr:
                            row_pri1 = row_tmp
                        else:
                            row_pri2 = row_tmp
                    elif 'Abis' in file_addr:
                        if '信令' in file_addr:
                            col_offset = 57
                            wrow = row_abis1
                        elif '测量' in file_addr:
                            col_offset = 87
                            wrow = row_abis2
                        elif '切换' in file_addr:
                            col_offset = 121
                            wrow = row_abis3
                        else:
                            col_offset = 152
                            wrow = row_abis4
                        row_tmp = self.write_excel(sheet, file_addr, excel_data, 0, row_ini, col_ini, col_offset, wrow)
                        if '信令' in file_addr:
                            row_abis1 = row_tmp
                        elif '测量' in file_addr:
                            row_abis2 = row_tmp
                        elif '切换' in file_addr:
                            row_abis3 = row_tmp
                        else:
                            row_abis4 = row_tmp
                    else:
                        if '信令' in file_addr:
                            col_offset = 175
                            wrow = row_a1
                        elif '切换' in file_addr:
                            col_offset = 204
                            wrow = row_a2
                        else:
                            col_offset = 233
                            wrow = row_a3
                        row_tmp = self.write_excel(sheet, file_addr, excel_data, 0, row_ini, col_ini, col_offset, wrow)
                        if '信令' in file_addr:
                            row_a1 = row_tmp
                        elif '切换' in file_addr:
                            row_a2 = row_tmp
                        else:
                            row_a3 = row_tmp

        workbook.save(self.excel_outputpath)

    def write_excel(self, sheet, file_addr, excel_data, sheet_num, row_ini, col_ini, col_offset, wrow):  # 写入表格函数
        single_excel = excel_data.sheet_by_index(sheet_num)  # 选择一个sheet作为读取文件
        sheet_name = excel_data.sheet_names()  # 获取sheet名字
        print("Target:" + file_addr + "\n" + "Sheet:" + sheet_name[sheet_num])
        for row in range(row_ini, single_excel.nrows):
            for col in range(col_ini, single_excel.ncols):
                values = single_excel.cell(row, col).value
                if isinstance(values, float):
                    values = int(values)
                else:
                    pass

                if values == '' or values is None:
                    pass
                else:
                    sheet.write(wrow, col + col_offset, u'%s' % values, self.style_dataRow)
            wrow = wrow + 1
        return wrow
    # 文档遍历，传入根目录
    def listdir(self, path):
        file_list = []
        for file in os.listdir(path):
            file_path = os.path.join(path, file)  # 绝对路径
            if os.path.isdir(file_path):  # 如果还是文件夹，继续迭代
                # if file_list:
                #     file_list.append(listdir(file_path))
                # else:
                #     file_list = listdir(file_path)
                file_list.append(self.listdir(file_path))
            elif os.path.splitext(file_path)[1] == '.xls' or os.path.splitext(file_path)[1] == '.xlsx' \
                    or os.path.splitext(file_path)[1] == '.txt':  # 判断文件是否为Excel
                file_list.append(file_path)
            else:
                None
        return file_list  # 返回Excel文件路径列表

    # 多层list拆分，传入为list
    def splitlist(self, file_list):
        alist = []
        a = 0

        for sublist in file_list:
            if isinstance(sublist, list):  # 判断是否迭代
                for i in sublist:
                    alist.append(i)
            else:
                alist.append(sublist)
        for i in alist:
            if type(i) == type([]):  # 判断是否还有列表
                a = + 1
                break
        if a == 1:
            return self.splitlist(alist)  # 还有列表，进行递归
        if a == 0:
            return alist


