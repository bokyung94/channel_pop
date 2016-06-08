Enter file contents here
# -*- coding: utf-8 -*-
'''CCP Log에서 각 채널 별로 로그를 분리 시키는 함수 선언, 분리된 로그는 채널이름, 날짜로 구분 되며
분리 이후 생성된 각 채널별 DB는 '날짜', '시간', '채널IP', 'join/leave', 'CM MAC', 'STB IP'의 값을 가지고 있음'''
'''Log file(ccp_log.csv)을 읽어서 Mcast별 DB를 반환함'''
import itertools
import csv
import sys
file_name = sys.argv[1:]
'''모듈 뒤에 분석 대상 파일을 지정할 수 있도록 매개 변수 선언'''
'''Log file을 읽어서 Mcast IP 리스트를 반환하는 함수 선언'''

def get_Index(file_name, property):
    with open(file_name, property) as log:
        McastIp = list()
        SgId = list()
        for line in log:
            LineValues = line.split(',')
            # 실행 시 7번째 열의 값을 갖고 있지 않은 경우 에러를 예방하기 위해 try/except 구문 사용
            try:
                McastIp.append(LineValues[7])
                SgId.append(LineValues[8])
                # 리스트 내의 중복 제거를 위해 set 구문 실행
                McastIp = list(set(McastIp))
                SgId = list(set(SgId))
            # 에러 발생 시 패스 시키고 다음 줄을 읽어 들임.
            except:
                pass
        return McastIp, SgId

# 상기 함수를 실행하여 Mcast List와 SGID List를 가짐(','로 구분하여 두 개의 리턴 값 리턴).
McastList, SgIdList = get_Index('Dobong_ccp_log.csv', 'r')

# Mcast별로 순환 시키기 위해 for 구문 지정
for i in range(len(SgIdList)):
    for j in range(len(McastList)):
        print("==========================================")
        print('Making DataBase for Multicast')
        print("==========================================")
        # Temp file 생성
        with open('Dobong_ccp_log.csv', 'r') as log:
            StbState = dict()
            Popularity = 0
            for line in log:
                LineValues = line.split(',')
                try:
                    # 원본 로그 한 줄을 읽어서 date, time, Mcast_IP, Join/Leave, CM_MAC, STB_IP 값을 변수로 받는다.
                    DateTime = LineValues[1]
                    Query = str(LineValues[3])
                    State = str(LineValues[4])
                    CmMac = str(LineValues[5])
                    StbIp = str(LineValues[6])
                    McastIp = str(LineValues[7])
                    SgId = str(LineValues[8])
                    CmType = str(LineValues[9])
                    CmModel = str(LineValues[10])

                    if SgId == SgIdList[i]:
                        if McastIp == McastList[j]:
                            # 위의 변수 중 Mcast_IP값을 McastList에서 뽑아낸 i번째 값과 대조 시킨다.
                            c = csv.writer(open(SgIdList[i]+"_"+McastList[j]+".csv", 'a', newline=''))
                            if Query == "JOIN" and (State == "ACCEPT" or State == "NOCHANGE"):
                                if StbState.get(StbIp) == "JOIN":
                                    pass
                                else:
                                    StbState.update({StbIp:Query})
                                    Popularity = Popularity + 1
                                    c.writerow([DateTime, Query, State, CmMac, StbIp, McastIp, SgId, CmType, CmModel, Popularity])
                                    c.close()

                            elif (Query == "LEAVE" or Query == "DROP") and (State == "ACCEPT"or State == "NOCHANGE"):
                                if StbState.get(StbIp) == "JOIN":
                                    StbState.update({StbIp:Query})
                                    Popularity = Popularity - 1
                                    c.writerow([DateTime, Query, State, CmMac, StbIp, McastIp, SgId, CmType, CmModel, Popularity])
                                    c.close()
                                else:
                                    pass
                            else:
                                pass

                except:
                    pass
