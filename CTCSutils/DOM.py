# -*- coding: utf-8 -*-
"""
@File    :  DOM.py
@Time    :  2022/3/30 17:40
@Author  :  changzheng
@Version :  1.0
@Desc    :  处理收到的XML报文中的信息
"""
import sys
from datetime import datetime
from xml.dom import minidom


class DOM(object):
    def __init__(self, train_Num=None, happen_time=None, time_first=None, time_last=None, content_flag=None, trans_id=None, req_result=None):
        # self.data_model = data_model

        self.train_Num = train_Num
        self.happen_time = happen_time
        self.time_first = time_first
        self.time_last = time_last
        self.content_Flag = content_flag
        self.trans_ID = trans_id
        self.req_Result = req_result

    def dom_readXML(self, msg):
        dom = minidom.parseString(msg)
        root = dom.documentElement
        '''
            获取车次号
        '''
        trainNum = root.getElementsByTagName('TrainNum')
        train_Num = trainNum[0].childNodes[0].nodeValue
        self.train_Num = train_Num
        '''
            请求数据内容时间
        '''
        happentime = root.getElementsByTagName('Happentime')
        happen_time = happentime[0].childNodes[0].nodeValue
        self.happen_time = happen_time

        timefirst = root.getElementsByTagName('Timefirst')
        time_first = timefirst[0].childNodes[0].nodeValue
        self.time_first = time_first

        timelast = root.getElementsByTagName('Timelast')
        time_last = timelast[0].childNodes[0].nodeValue
        self.time_last = time_last
        '''
            请求内容标志
            1为分析请求；
            2为请求确认；
            3为故障分析完成通知；
            4为故障分析完成通知确认；
            5为故障分析进度提示
        '''
        contentFlag = root.getElementsByTagName('ContentFlag')
        content_Flag = contentFlag[0].childNodes[0].nodeValue
        self.content_Flag = content_Flag
        '''
            获取请求事务唯一编号，保证全系统唯一
            用于区分同一时刻有多个不同的请求，方便区分事务。智能分析系统的事务唯一编号取自接口监测请求消息携带的事务编号。
        '''
        transID = root.getElementsByTagName('TransID')
        trans_ID = transID[0].childNodes[0].nodeValue
        self.trans_ID = trans_ID
        '''
            请求结果
            0：接收到查询请求，并能正常分析；
            1：接收查询请求，超过最大请求数，予以拒绝（支持30个请求）；
            3：分析进度值
        '''
        reqResult = root.getElementsByTagName('ReqResult')
        req_Result = reqResult[0].childNodes[0].nodeValue
        self.req_Result = req_Result

    '''
        获得收到XML文件中的TrainNum、Happentime、ContentFlag、TransID、ReqResult
    '''

    def getTrainNum(self):
        return self.train_Num

    def getHappenTime(self):
        return self.happen_time

    def getContentFlag(self):
        return self.content_Flag

    def getTransID(self):
        return self.trans_ID

    def getReqResult(self):
        return self.req_Result
    def getTimeFirst(self):
        return self.time_first
    def getTimeLast(self):
        return self.time_last

    def dom_writeXML(self, content_Flag, req_Result):
        dom = minidom.getDOMImplementation().createDocument(None, 'CommMsg', None)
        root = dom.documentElement

        element = dom.createElement('TrainNum')
        element.appendChild(dom.createTextNode(self.train_Num))
        root.appendChild(element)

        element = dom.createElement('Happentime')

        now = datetime.now()
        element.appendChild(dom.createTextNode(now.strftime("%Y-%m-%d %H:%M:%S")))
        root.appendChild(element)

        element = dom.createElement('Timefirst')
        element.appendChild(dom.createTextNode(self.time_first))
        root.appendChild(element)

        element = dom.createElement('Timelast')
        element.appendChild(dom.createTextNode(self.time_last))
        root.appendChild(element)

        # element = dom.createElement('Datamodel')
        # element.appendChild(dom.createTextNode(self.data_model))
        # root.appendChild(element)

        element = dom.createElement('ContentFlag')
        element.appendChild(dom.createTextNode(content_Flag))
        root.appendChild(element)

        element = dom.createElement('TransID')
        element.appendChild(dom.createTextNode(self.trans_ID))
        root.appendChild(element)

        element = dom.createElement('ReqResult')
        element.appendChild(dom.createTextNode(req_Result))
        root.appendChild(element)

        # xmlStdin = XmlStdin()
        # sys.stdin = xmlStdin
        # dom.writexml(sys.stdin, addindent='\t', newl='\n', encoding='utf-8')
        # print(xmlStdin.toString())
        with open('default.xml', 'w', encoding='utf-8') as f:
            dom.writexml(f, addindent='\t', newl='\n', encoding='utf-8')
        dom_string = open('default.xml', encoding = 'utf-8').read()
        # print(xmlStdin.toString())
        # return xmlStdin.toString()
        return dom_string
        # return xmlStdin.toString()

    def write_output_xml(slef,addr, TrainNum, Happentime, TransID, ExcepType, AbisInfoData, AbisInfoDes, AInfoData, AInfoDes,
                         PRIInfoData, PRIInfoDes, AnalyseDes, ExcepReasonDes):
        # AbisMRData, AbisMRDes,
        dom = minidom.getDOMImplementation().createDocument(None, 'AnalyseRes', None)
        root = dom.documentElement

        element = dom.createElement('TrainNum')
        element.appendChild(dom.createTextNode(TrainNum))
        root.appendChild(element)

        element = dom.createElement('Happentime')
        element.appendChild(dom.createTextNode(Happentime))
        root.appendChild(element)

        element = dom.createElement('TransID')
        element.appendChild(dom.createTextNode(TransID))
        root.appendChild(element)

        element = dom.createElement('ExcepType')
        element.appendChild(dom.createTextNode(ExcepType))  # 故障类型枚举值，需智能分析系统给出枚举值对应的故障类型名称
        root.appendChild(element)

        element = dom.createElement('AbisInfoData')
        element.appendChild(dom.createTextNode(AbisInfoData))  # Abis接口关注的信令行号，多行用”&”符号拼接
        root.appendChild(element)

        element = dom.createElement('AbisInfoDes')
        element.appendChild(dom.createTextNode(AbisInfoDes))  # Abis接口信令描述，对Abis接口的信令特征进行说明
        root.appendChild(element)

        element = dom.createElement('AInfoData')
        element.appendChild(dom.createTextNode(AInfoData))  # A接口关注的数据行号，多行用”&”符号拼接
        root.appendChild(element)

        element = dom.createElement('AInfoDes')
        element.appendChild(dom.createTextNode(AInfoDes))  # A接口信令描述，对A接口的信令特征进行说明
        root.appendChild(element)

        element = dom.createElement('PRIInfoData')
        element.appendChild(dom.createTextNode(PRIInfoData))  # PRI接口关注的数据行号，多行用”&”符号拼接
        root.appendChild(element)

        element = dom.createElement('PRIInfoDes')
        element.appendChild(dom.createTextNode(PRIInfoDes))  # PRI接口数据描述，对PRI接口的数据特征进行说明
        root.appendChild(element)

        element = dom.createElement('AnalyseDes')
        element.appendChild(dom.createTextNode(AnalyseDes))  # 多接口数据汇总分析说明
        root.appendChild(element)

        element = dom.createElement('ExcepReasonDes')
        element.appendChild(dom.createTextNode(ExcepReasonDes))  # 故障原因说明
        root.appendChild(element)

        with open(addr, 'w', encoding='utf-8') as f:
            dom.writexml(f, addindent='\t', newl='\n', encoding='utf-8')

class XmlStdin():
    def __init__(self):
        self.str = ""

    def write(self, value):
        self.str += value

    def toString(self):
        return self.str
