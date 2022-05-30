"""
    交大的在帧类型中找DISCONNECT，其他在子类型中找DISCONNECT，然后将对应的时间输出
"""
import xlrd
import re
from datetime import datetime, timedelta


def getTimeByType(file_path, my_model):
    excel_file = xlrd.open_workbook(file_path)
    sheet = excel_file.sheet_by_index(0)
    col_time = 0
    row_index = 0
    col_type = 0
    if my_model == '交大':
        for i in range(sheet.ncols):
            if sheet.cell_value(0, i).find("PRI_挂机时间_2") != -1 :
                col_time = i
                break
        for i in range(sheet.ncols):
            if sheet.cell_value(0, i).find("PRI_话单结果_2") != -1:
                col_type = i
                break
        for i in range(sheet.nrows):
            if sheet.cell_value(i, col_type) == "无156包和39包":
                row_index = i
                break
        raw_time = sheet.cell_value(row_index, col_time)
        end_time = re.sub(":| ", "-", raw_time[:raw_time.find(".")])
    elif my_model == '铁科':
        for i in range(sheet.ncols):
            if sheet.cell_value(0, i).find("PRI_触发时间_1") != -1:
                col_time = i
                break
        for i in range(sheet.ncols):
            if sheet.cell_value(0, i).find("PRI_信令类型_1") != -1:
                col_type = i
                break
        for i in range(sheet.nrows-1, 0, -1):
            # print(sheet.cell_value(i, col_type))
            if sheet.cell_value(i, col_type) == "DISCONNECT":
                row_index = i
                break
        raw_time = sheet.cell_value(row_index, col_time)
        raw_time_tmp = raw_time.rsplit(' ', 1)[0]
        end_time = re.sub(":| ", "-", raw_time_tmp)
    else:
        for i in range(sheet.ncols):
            if sheet.cell_value(0, i).find("PRI_触发时间_1") != -1:
                col_time = i
                break
        for i in range(sheet.ncols):
            if sheet.cell_value(0, i).find("PRI_子类型_1") != -1:
                col_type = i
                break
        for i in range(sheet.nrows-1, 0, -1):
            if sheet.cell_value(i, col_type) == "DISCONNECT":
                row_index = i
                break
        raw_time = sheet.cell_value(row_index, col_time)
        end_time = re.sub(":| ", "-", raw_time[:raw_time.find(".")])
    end_time = datetime.strptime(end_time, "%Y-%m-%d-%H-%M-%S")
    end_time = end_time + timedelta(seconds=1)
    start_time = end_time - timedelta(minutes=1)
    return start_time.strftime("%Y-%m-%d-%H-%M-%S"), end_time.strftime("%Y-%m-%d-%H-%M-%S")
