# -*- coding: utf-8 -*-
"""
@File    :  databaseService.py
@Time    :  2022/3/31 19:37
@Author  :  changzheng
@Version :  1.0
@Desc    :  负责和数据库进行交互
"""

import CTCSutils.Constants
import pymysql
from Dao.recordDao import *


# 做成单例
class DatabaseService:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def getRecordById(self, id):
        pass

    def login(self, username, password):
        if username == '' or password == '':
            return None
        try:
            self.conn = pymysql.connect(host='localhost', user=username, password=password, charset='utf8')
        except Exception:
            return CTCSutils.Constants.LOGIN_FAIL
        if self.conn:
            self.cursor = self.conn.cursor()
            self.createDataBase()
            # todo 未来删除
            self.createTable("hello")
            return CTCSutils.Constants.LOGIN_SUCCESS
        return CTCSutils.Constants.UNKONWN_ERROR

    def createDataBase(self):
        if not self.cursor:
            return CTCSutils.Constants.UNCONNECT_DATABASE
        createDatabaseSQL = "CREATE DATABASE IF NOT EXISTS hello CHARACTER SET utf8"
        self.cursor.execute(createDatabaseSQL)
        useDatabaseSQL = "USE hello"
        self.cursor.execute(useDatabaseSQL)

    def createTable(self, tableName):
        if not self.cursor:
            return CTCSutils.Constants.UNCONNECT_DATABASE
        # 车次号 表名 日期 CTCS_ID 线路 公里标 故障类型 切换前小区 切换后小区
        createTableSQL = """
        CREATE TABLE IF NOT EXISTS {}(
            id VARCHAR(64),
            trainId VARCHAR(32),
            tableName VARCHAR(64),
            dataTime DATE,
            CTCS_ID VARCHAR(64),
            line VARCHAR(32),
            kilometerMarker VARCHAR(64),
            faultType VARCHAR(32),
            switchBeforeCell VARCHAR(64),
            switchAfterCell VARCHAR(64)
        )
        """.format(tableName)
        self.cursor.execute(createTableSQL)

    def findRecordById(self, tableName, Id):
        if not self.cursor:
            return CTCSutils.Constants.UNCONNECT_DATABASE
        return findRecordById(self.cursor, tableName, Id)

    def detect_sql(self, mysql_username, mysql_password):  # 初始化函数
        # 新建不同故障类型的数据库
        DB_NAME = ['故障案例', 'frmr类型wx', 'frmr类型y', 'frmr类型z', 'er类型', 'm156类型', 'di_dr类型', 'msc类型', '安全层mac',
                   'sapdu长度错误', '不明原因']
        detect_sign = 'False'

        try:
            connect = pymysql.connect(host='localhost'
                                      , user=mysql_username
                                      , password=mysql_password
                                      )  # 登录
        except pymysql.err.OperationalError:
            detect_sign = 'Error'  # 若出现错误则返回error
            return detect_sign

        for name in DB_NAME:
            sql = "show databases like '%s'" % name
            with connect.cursor() as cursor:
                cursor.execute(sql)
                row = cursor.fetchone()  # 抓取是否含有该数据库
                if row:
                    detect_sign = 'True'
                else:
                    continue

        connect.close()
        return detect_sign

    def show_sql(self, mysql_username, mysql_password, sqlname):  # 显示数据库，与第一个函数类似
        try:
            connect = pymysql.connect(host='localhost'
                                      , user=mysql_username
                                      , db=sqlname
                                      , password=mysql_password
                                      )
        except pymysql.err.OperationalError:
            table_list = 'Error'
            return table_list

        table_list = []
        cursor = connect.cursor()
        sql = 'SHOW TABLES'
        cursor.execute(sql)
        result = cursor.fetchall()
        for i in range(len(result)):
            table_list.append(result[i][0])

        connect.close()
        return table_list

    def delete_sql(self, mysql_username, mysql_password, sqlname, filename):  # 删除数据库，与第一个函数类似
        try:
            connect = pymysql.connect(host='localhost'
                                      , user=mysql_username
                                      , db=sqlname
                                      , password=mysql_password
                                      )
        except pymysql.err.OperationalError:
            return 'Error'

        cursor = connect.cursor()
        sql = "DROP TABLE IF EXISTS `" + filename + "`"
        cursor.execute(sql)
        connect.close()
        return True

    def initsql_run(self, mysql_username, mysql_password, is_continue):  # 与第一个函数类似

        DB_NAME = ['故障案例', 'frmr类型wx', 'frmr类型y', 'frmr类型z', 'er类型', 'm156类型', 'di_dr类型', 'msc类型', '安全层mac',
                   'sapdu长度错误', '不明原因', 'sim卡脱网']

        connect = pymysql.connect(host='localhost'
                                  , user=mysql_username
                                  , password=mysql_password
                                  )

        for name in DB_NAME:
            sql = "show databases like '%s'" % name
            with connect.cursor() as cursor:
                cursor.execute(sql)
                if is_continue:
                    sql = 'DROP DATABASE IF EXISTS %s' % name
                    cursor.execute(sql)
                    connect.commit()
                else:
                    continue

                sql = "CREATE DATABASE `%s` CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_unicode_ci';" % name
                cursor.execute(sql)
                connect.commit()

        connect.close()

# 为了完成单例，这样全局从这里引用的就是一个对象
databaseService = DatabaseService()
