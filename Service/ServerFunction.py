# -*- coding: utf-8 -*-
"""
@File    :  ServerFunction.py
@Time    :  2022/5/3 10:00
@Author  :  zhuyugang
@Version :  1.0
@Desc    :  完成客户端网络的各种连接、处理
"""

import re
from sklearn.ensemble import RandomForestClassifier
from sklearn import tree
from sklearn.metrics import accuracy_score
import pandas as pd
# import xgboost as xgb
import numpy as np
from sklearn.model_selection import train_test_split
import pymysql

users = {}

from Service.DatabaseService import DatabaseService


def window_main(root_in, pw):  # 主窗口
    global var_model
    detect_window = DatabaseService().detect_sql(root_in, pw)
    if detect_window == 'Error':
        print('用户名或密码错误！')
        return 0
    else:
        # denglu.destroy()
        print("*****************************\n"
              "          登录成功\n"
              "*****************************\n")  # 打印信息（终端）


def init_subwindow(mysql_username, mysql_passowrd):  # 数据初始化界面
    global subwindow
    # 闭环处理
    detect_sgn = DatabaseService().detect_sql(mysql_username, mysql_passowrd)  # 登录数据库
    if detect_sgn == 'True' or detect_sgn == 'False':  # 判断密码是否正确
        pass
    elif detect_sgn == 'Error':
        print('用户名或密码错误！')  # 错误输出
        return 0
    else:
        print('未知错误！')  # 其他错误
        return 0

    # 建立数据库
    if detect_sgn == 'True':
        print('存在数据库，即将覆盖...')

        print('正在重建数据库......\n')
        DatabaseService().initsql_run(mysql_username, mysql_passowrd, is_continue=True)
        print('Complete!\n')

    if detect_sgn == 'False':
        print('正在重建数据库......\n')
        DatabaseService().initsql_run(mysql_username, mysql_passowrd, is_continue=True)
        print('Complete!\n')


'''历史搜索部分'''


def btn_click(train_num, root_in, pw):
    global record_str
    detect_his = DatabaseService().detect_sql(root_in, pw)
    his_model = '车次号'
    if detect_his == 'Error':
        print('用户名或密码错误！')
        return 0
    else:
        pass

    try:
        TB1, TB2 = cch_ctcs_id(root_in, pw)  # 根据输入的CTCS_ID或车次号得到历史信息
        tr_n = fuzzy_search(train_num, TB1, TB2, his_model)  # 将得到的历史信息输入到表格中显示
        tr_n = str(tr_n[0]).split("\'")[1]
        GZ, LS, SJ, CTCS_ID, XL, GLB, QHQ_XQ, QHH_XQ = search_history(root_in, pw, tr_n, his_model)
        record_str = ''
        for ii in range(len(GZ)):
            ii_tmp = str(ii + 1)
            record_str_temp = '**********第 %s 条数据*********************\n' % ii_tmp + '表名：    %s\n' % LS[
                ii] + '日期：    %s\n' % SJ[
                                  ii] + 'CTCS_ID： %s\n' % CTCS_ID[ii] + '线路：     %s\n' % XL[ii] + '公里标：   %s\n' % GLB[
                                  ii] + '故障类型： %s\n' % GZ[ii] + '切换前小区：%s\n' % QHQ_XQ[ii] + '切换后小区：%s\n' % QHH_XQ[
                                  ii] + '*****************************************\n'
            record_str = record_str_temp + record_str
        return record_str
    except Exception:
        return ("*****************************\n" +
                "   车次号输入错误或无该车次！！！\n" +
                "*****************************\n")


def cch_ctcs_id(root_in, pw):  # 历史信息查询
    conn = pymysql.connect(host='localhost', user='%s' % root_in, password='%s' % pw, database='frmr类型y')
    cur = conn.cursor()
    cur.execute('SHOW DATABASES')
    data_a = list(cur.fetchall())
    data_all = []
    for ii in range(len(data_a)):
        if str(data_a[ii]) == "('di_dr类型',)" or str(data_a[ii]) == "('er类型',)" \
                or str(data_a[ii]) == "('frmr类型wx',)" or str(data_a[ii]) == "('frmr类型y',)" \
                or str(data_a[ii]) == "('frmr类型z',)" or str(data_a[ii]) == "('m156类型',)" \
                or str(data_a[ii]) == "('msc类型',)" or str(data_a[ii]) == "('sapdu长度错误',)" \
                or str(data_a[ii]) == "('sim卡脱网',)" or str(data_a[ii]) == "('安全层mac',)" \
                or str(data_a[ii]) == "('不明原因',)" or str(data_a[ii]) == "('故障案例',)":
            data_all = data_all + list(data_a[ii])
        else:
            continue
    for ii in range(len(data_all)):
        data_al = []
        data_al.append(data_all[ii])
        data_all[ii] = tuple(data_al)
    TB1 = []
    TB2 = []
    for i in data_all:  # 数据库名称（即故障类型）
        conn.select_db(i[0])
        cur.execute('SHOW TABLES')
        ret = cur.fetchall()
        for j in ret:
            tb1 = cur.execute("select * from `" + j[0] + "` where `PRI_车次号_1` is not NULL")
            if tb1 == 1:
                cur.execute("select PRI_车次号_1 from `" + j[0] + "`")
                tb11 = cur.fetchall()
                TB1 = TB1 + list(tb11)
            tb2 = cur.execute("select * from `" + j[0] + "` where `PRI_CTCS_ID_1` is not NULL")
            if tb2 == 1:
                cur.execute("select PRI_CTCS_ID_1 from `" + j[0] + "`")
                tb22 = cur.fetchall()
                TB2 = TB2 + list(tb22)
        for s in TB1:
            if TB1.count(s) > 1:
                TB1.remove(s)
        for ss in TB2:
            if TB2.count(ss) > 1:
                TB2.remove(ss)
    return (TB1, TB2)


def fuzzy_search(t_num, TB1, TB2, history_model):
    suggestions = []
    pattern = '.*'.join(t_num)
    regex = re.compile(pattern)
    if history_model == '车次号':
        for item in TB1:
            match = regex.search(str(item))
            if match:
                suggestions.append(str(item))
    elif history_model == 'CTCS_ID':
        for item in TB2:
            match = regex.search(str(item))
            if match:
                suggestions.append(str(item))
    return suggestions


def search_history(root_in, pw, train_num, history_model):
    conn = pymysql.connect(host='localhost', user='%s' % root_in, password='%s' % pw, database='frmr类型y')
    cur = conn.cursor()
    cur.execute('SHOW DATABASES')
    data_a = list(cur.fetchall())
    data_all = []
    for ii in range(len(data_a)):
        if str(data_a[ii]) == "('di_dr类型',)" or str(data_a[ii]) == "('er类型',)" \
                or str(data_a[ii]) == "('frmr类型wx',)" or str(data_a[ii]) == "('frmr类型y',)" \
                or str(data_a[ii]) == "('frmr类型z',)" or str(data_a[ii]) == "('m156类型',)" \
                or str(data_a[ii]) == "('msc类型',)" or str(data_a[ii]) == "('sapdu长度错误',)" \
                or str(data_a[ii]) == "('sim卡脱网',)" or str(data_a[ii]) == "('安全层mac',)" \
                or str(data_a[ii]) == "('不明原因',)" or str(data_a[ii]) == "('故障案例',)":
            data_all = data_all + list(data_a[ii])
        else:
            continue
    for ii in range(len(data_all)):
        data_al = []
        data_al.append(data_all[ii])
        data_all[ii] = tuple(data_al)
    GZ = '('
    LS = '('
    SJ = []
    CTCS_ID = []
    XL = '('
    GLB = []
    QHQ_XQ = []
    QHH_XQ = []
    for i in data_all:  # 数据库名称（即故障类型）
        conn.select_db(i[0])
        cur.execute('SHOW TABLES')
        ret = cur.fetchall()
        for j in ret:  # 查找车次号
            if history_model == '车次号':
                tb = cur.execute("select * from `" + j[0] + "` where `PRI_车次号_1` = '%s'" % train_num)
            elif history_model == 'CTCS_ID':
                tb = cur.execute("select * from `" + j[0] + "` where `PRI_CTCS_ID_1` = '%s'" % train_num)
            else:
                break
            if tb == 1:
                xl_1 = j[0].split('-')
                XL += "'" + xl_1[2] + "'" + ', '

                cur.execute("select PRI_触发时间_1 from `" + j[0] + "`")
                sj = cur.fetchall()
                SJ = SJ + list(sj)

                cur.execute("select PRI_CTCS_ID_1 from `" + j[0] + "`")
                ctcs_id = cur.fetchall()
                CTCS_ID = CTCS_ID + list(ctcs_id)

                cur.execute("select PRI_公里标_1 from `" + j[0] + "`")
                glb = cur.fetchall()
                GLB = GLB + list(glb)

                cur.execute("select Abis_切换前小区_3 from `" + j[0] + "`")
                qhq_xq = cur.fetchall()
                QHQ_XQ = QHQ_XQ + list(qhq_xq)

                cur.execute("select Abis_切换后小区_3 from `" + j[0] + "`")
                qhh_xq = cur.fetchall()
                QHH_XQ = QHH_XQ + list(qhh_xq)

                GZ += "'" + i[0] + "'" + ', '
                LS += "'" + j[0] + "'" + ', '
    XL = eval(XL + ')')
    GZ = eval(GZ + ')')
    LS = eval(LS + ')')
    return (GZ, LS, SJ, CTCS_ID, XL, GLB, QHQ_XQ, QHH_XQ)


''' 历史搜索部分end  '''

'''准确度'''


def AI_Classify():  # 智能搜索可视化

    Tree_C, For_C = jqxx_pre()  # 获取两种算法的准确率结果
    accuracy_str = '*******************************\n' + '测试集/数据集  决策树   随机森林\n' + '2/10         ' + '%-8s' % Tree_C[
        0] + '%-6s\n' % For_C[0] + '3/10         ' + '%-8s' % Tree_C[1] + '%-6s\n' % For_C[
                       1] + '4/10         ' + '%-8s' % \
                   Tree_C[2] + '%-6s\n' % For_C[2] + '5/10         ' + '%-8s' % Tree_C[3] + '%-6s\n' % For_C[
                       3] + '*******************************\n'
    return accuracy_str


def jqxx_pre():  # 算法准确率获取

    datasets = pd.read_csv('demo_2.csv', engine='python', error_bad_lines=False, encoding='unicode_escape')  # 获取样本库
    Train_X = datasets.iloc[1:31, 0:13].values  # 训练样本的特征
    Train_y = datasets.iloc[1:31, 14].values  # 训练样本的分类结果
    TEST_SIZE = [0.2, 0.3, 0.4, 0.5]
    Tree_C = []
    For_C = []
    XGB_C = []
    for ii in range(len(TEST_SIZE)):
        X_Train, X_Test, Y_Train, Y_Test = train_test_split(Train_X, Train_y, test_size=TEST_SIZE[ii],
                                                            random_state=0)  # 将数据按照特定比例分成训练集与测试集
        # 决策树
        clf_tree = tree.DecisionTreeClassifier()  # 调用决策树函数
        clf_tree.fit(X_Train, Y_Train)  # 输入训练集
        tree_predictions = clf_tree.predict(X_Test)  # 获得新数据测试集的结果
        error_r = np.sum(tree_predictions != Y_Test) / Y_Test.shape[0]  # 计算错误率
        Tree = 1 - error_r  # 准确率
        Tree = int(Tree * 1000) / 1000
        Tree_C.append(Tree)
        # 随机森林
        clf = RandomForestClassifier(n_estimators=10, max_depth=None, random_state=0)
        clf.fit(X_Train, Y_Train)
        test_predictions = clf.predict(X_Test)
        AA = accuracy_score(Y_Test, test_predictions)
        AA = int(AA * 1000) / 1000
        For_C.append(AA)
    return Tree_C, For_C
