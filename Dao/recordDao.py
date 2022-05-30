# -*- coding: utf-8 -*-
"""
@File    :  recordDao.py
@Time    :  2022/3/31 19:36
@Author  :  changzheng
@Version :  1.0
@Desc    :  负责和数据库交互，获取对应记录的数据
"""


def findRecordById(cursor, tableName, Id):
    findRecordSQL = "SELECT * FROM {} WHERE id={}".format(tableName, Id)
    cursor.execute(findRecordSQL)
    return cursor.fetchone()
