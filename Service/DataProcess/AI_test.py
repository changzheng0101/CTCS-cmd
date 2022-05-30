from sklearn.ensemble import RandomForestClassifier
from sklearn import tree
import pandas as pd
# import xgboost as xgb

def Feature_Classify(XX):  # 两种算法的结果
    datasets = pd.read_csv('demo_2.csv', engine='python', encoding='gbk')  # 载入模型样本库
    X_Train = datasets.iloc[0:, 0:14].values  # 载入样本数据集的特征
    Y_Train = datasets.iloc[0:, -1].values  # 载入样本数据集的分类结果
    # 决策树
    clf_tree = tree.DecisionTreeClassifier()  # 决策树算法
    clf_tree.fit(X_Train, Y_Train)  # 决策树训练结果
    A = clf_tree.predict(XX)  # 对新数据的预测分类
    # 随机森林
    clf_for = RandomForestClassifier(n_estimators=10, max_depth=None, random_state=0)  # 随机森林算法
    clf_for.fit(X_Train, Y_Train)  # 随机森林训练结果
    B = clf_for.predict(XX)  # 对新数据的预测分类
    '''
    # XGboost
    xgb_train = xgb.DMatrix(X_Train, label=Y_Train)
    params = {'objective': 'multi:softmax', 'eta': 0.1, 'max_depth': 5, 'num_class': 10}
    num_round = 30
    bst = xgb.train(params, xgb_train, num_round)
    C = clf_for.predict(XX)
    '''
    AA = [A,B]  # 将分类结果一一对应到相应的故障类型
    FF_C = []
    BB = []
    for ii in range(len(AA)):
        if AA[ii] == 0:
            AA_C = '小区切换失败'
            B = 0
        elif AA[ii] == 1:
            AA_C = '小区切换后上行质量突降'
            B = 1
        elif AA[ii] == 2:
            AA_C = '非小区切换，上行电平无效'
            B = 2
        elif AA[ii] == 3:
            AA_C = '上行电平正常，上行质差(2级-3级)，无下行及邻区电平值'
            B = 3
        elif AA[ii] == 4:
            AA_C = '小区切换后上行电平、通信质量正常，下行电平正常，下行质量突降，NBO正常'
            B = 4
        elif AA[ii] == 5:
            AA_C = '小区切换后上行电平、通信质量正常，下行电平正常，下行质量突降，NBO-110'
            B = 5
        elif AA[ii] == 6:
            AA_C = 'V.110失步'
            B = 6
        elif AA[ii] == 7:
            AA_C = 'CSD链路问题'
            B = 7
        elif AA[ii] == 8:
            AA_C = 'T1启用机制问题'
            B = 8
        # elif AA[ii] == 9:
        #     AA_C = 'RBC发送P42引起超时'
        #     B = 9
        elif AA[ii] == 10:
            AA_C = 'GSM-R网络和车载数据未发现异常，RBC断开连接与ISDN服务器'
            B = 10
        elif AA[ii] == 11:
            AA_C = '底层信息交互正常，但应用层不发送消息'
            B = 11
        elif AA[ii] == 12:
            AA_C = '连续小区切换失败'
            B = 12
        else:
            AA_C = '未知故障'
            B = 12
        FF_C.append(AA_C)
        BB.append(B)
    if BB[0] == BB[1]:
        BC = BB[0]
    else:
        BC = BB[1]
    return FF_C, BC
'''
    if A == 0:
        AA_C = '小区切换失败'
        B = 0
    elif A == 1:
        AA_C = '小区切换后上行质量突降'
        B = 1
    elif A == 2:
        AA_C = '非小区切换，上行电平无效'
        B = 2
    elif A == 3:
        AA_C = '上行电平、通信质量正常，无下行及邻区电平值'
        B = 3
    elif A == 4:
        AA_C = '小区切换后上行电平、通信质量正常，下行电平正常，下行质量突降，NBO正常'
        B = 4
    elif A == 5:
        AA_C = '小区切换后上行电平、通信质量正常，下行电平正常，下行质量突降，NBO-110'
        B = 5
    elif A == 6:
        AA_C = 'V.110失步'
        B = 6
    else:
        AA_C = '未知故障'
        B = 7
'''
