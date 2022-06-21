# -*- coding: utf-8 -*-
"""
@File    :  KeyWordSearch.py
@Time    :  2022/3/31 19:27
@Author  :  changzheng
@Version :  1.0
@Desc    :  搜索关键字
"""
from Service.MalfunctionJudge.TH.THJudgeService import THJudgeService
from Service.MalfunctionJudge.TK.TKJudgeService import TKJudgeService
from Service.MalfunctionJudge.JD.JDJudgeService import JDJudgeService

class KeyWordSearch(object):
    def __init__(self):
        pass

    def search_run(self, my_model, file_name, file_name_new, mysql_username, mysql_password):
        # search
        print("***********************************关键词搜索***********************************")

        downloadpath = "./data/excel_time"  # download path

        return_str = []
        excel_inputpath = downloadpath + '/' + file_name_new + '.xls'
        if my_model == "通号":
            error_str, time_direc_tab, isChannelSwitch, PRI_rowNum, Abis_rowNum, A_rowNum = THJudgeService(mysql_username, mysql_password).searchkey_updatesql(
                file_name, file_name_new, excel_inputpath, my_model)
        elif my_model == "铁科":
            error_str, time_direc_tab, isChannelSwitch, PRI_rowNum, Abis_rowNum, A_rowNum = TKJudgeService(mysql_username, mysql_password).searchkey_updatesql(
                file_name,
                file_name_new,
                excel_inputpath,
                my_model)
        else:
            error_str, time_direc_tab, isChannelSwitch, PRI_rowNum, Abis_rowNum, A_rowNum = JDJudgeService(mysql_username, mysql_password).searchkey_updatesql(
                file_name,
                file_name_new,
                excel_inputpath,
                my_model)
        if error_str != '不明原因' and error_str != '没有故障类型':
            return_str.append(file_name + '\n**************************\n   ' + error_str + '\n   ' +isChannelSwitch + '\n   '+time_direc_tab[0].split('；', -1)[1]+'**************************')
        elif error_str == '没有故障类型':
            return_str.append(file_name + '\n**************************\n' + '没有故障类型，请进一步人工分析！' + '\n**************************')
        else:
            return_str.append(file_name + '\n**************************\n   ' + error_str + '\n**************************')
        if error_str == 'no data':  # 不属于原因不明，属于没有数据的故障
            print("[M] unclassified : " + file_name)
        else:
            print("[M] classified : " + file_name)

        return return_str, time_direc_tab, PRI_rowNum, Abis_rowNum, A_rowNum, error_str
