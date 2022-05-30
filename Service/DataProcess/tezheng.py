
import csv
from Service.MalfunctionJudge.JD.JDAIService import JDAIService
from Service.MalfunctionJudge.TH.THAIService import THAIService
from Service.MalfunctionJudge.TK.TKAIService import TKAIService


def aaaa(example, mode):
    if mode == '交大':
        a_tz, A_TZ, A_time_dire,  PRI_rowNum, Abis_rowNum, PRI_infodes, Abis_infodes = JDAIService().SearchEigenvalues(example, mode)
    elif mode == '通号':
        a_tz, A_TZ, A_time_dire, PRI_rowNum, Abis_rowNum, PRI_infodes, Abis_infodes = THAIService().SearchEigenvalues(example, mode)
    else:
        a_tz, A_TZ, A_time_dire, PRI_rowNum, Abis_rowNum, PRI_infodes, Abis_infodes = TKAIService().SearchEigenvalues(
            example, mode)

    return a_tz, A_TZ, A_time_dire, PRI_rowNum, Abis_rowNum, PRI_infodes, Abis_infodes

def abab(a_tz):
    with open(r'demo_2.csv', mode='a', newline='', encoding='utf8') as cfa:
        wf = csv.writer(cfa)
        wf.writerow(a_tz)
