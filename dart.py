# coding=utf-8 #
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
import webbrowser
import parser
import requests
import lxml.html
import re
import psycopg2
from django.db import DataError
import time

def remove_letter(base_string,letter_remove):  # 문자열에서 선택된 특정 문자를 없애버리기
    letter_remove = letter_remove[0]
    string_length = len(base_string)
    location = 0

    while (location < string_length) :
        if base_string[location] == letter_remove:
            base_string = base_string[:location] + base_string[location+1::]  # [:a] -> 처음부터 a위치까지, [a::]a위치부터 끝
            string_length = len(base_string)
        location+= 1
    # print("Result: %s",base_string)
    return base_string

def searchitem():  # itemname으로 checkitem테이블을 뒤져서 index반환
    print("searchitem 1.1 : %s seconds ---" % (time.time() - start_time))
    selecteditemid =10000
    con = psycopg2.connect("dbname='webcrolldb' user='postgres' host='localhost' password='1111'")
    selectsql = " select item_id, itemname, val1, val2, val3, val4, val5 from checkitem; "
    cur = con.cursor()
    cur.execute(selectsql)
    itemrows = cur.fetchall()

    cur.close()
    con.close()
    return itemrows

# start_date와 end_date사이에 특정 기업의 보고서 리스트 조회하여 data(dataframe)에 저장하여 리턴
def searchreportlist(company_code,start_date,end_date):
    auth_key="f8ffbf4ce6492a40c97e739df4dea397683814a1" #authority key

    url = "http://dart.fss.or.kr/api/search.xml?auth="+auth_key+"&crp_cd="+company_code+"&start_dt="+start_date+"&end_dt="+end_date+"&bsn_tp=A001&bsn_tp=A002&bsn_tp=A003"

    print(url);
    resultXML=urlopen(url) #this is for response of XML
    result=resultXML.read() #Using read method
    xmlsoup=BeautifulSoup(result,'html.parser')

    data = pd.DataFrame()
    te=xmlsoup.findAll("list")
    for t in te:
        temp=pd.DataFrame(([[t.crp_cls.string,t.crp_nm.string,t.crp_cd.string,t.rpt_nm.string, t.rcp_no.string,t.flr_nm.string,t.rcp_dt.string, t.rmk.string]]), columns=["crp_cls","crp_nm","crp_cd","rpt_nm","rcp_no","flr_nm","rcp_dt","rmk"])
        data=pd.concat([data,temp])
    data=data.reset_index(drop=True)
    #print(data.iloc[:,[1,3,4,5,6,7]])
    #print('data type')
    #print(type(data))
    if len(data) > 0: #dataframe의 null체크를 len()함수로 수행함
        data=data[data.rmk!='정연'] #기재정정이 발생하면 기 보고서의 rmk컬럼에 "정"값이 들어가기 때문에 해당 보고서를 list에서 제외하는 로직 추가
        data =data[data.rmk != '정']
        data = data.reset_index(drop=True) # dataframe에서 행을 제거하면 해당 행이 null로 남기 때문에 reset_index로 해당 행을 제거

    if len(data) > 0:
        data['removeyn'] = data['rpt_nm'].str.find(sub='첨부정정')  # dataframe의 리포트 이름에 첨부정정이 있으면 제거
        data = data[data.removeyn < 0]
        data = data.reset_index(drop=True)  # dataframe에서 행을 제거하면 해당 행이 null로 남기 때문에 reset_index로 해당 행을 제거
        data['removeyn'] = data['rpt_nm'].str.find(sub='연장신고서')  # dataframe의 리포트 이름에 첨부정정이 있으면 제거
        data = data[data.removeyn < 0]
        data = data.reset_index(drop=True)  # dataframe에서 행을 제거하면 해당 행이 null로 남기 때문에 reset_index로 해당 행을 제거
    return data

# 보고서리스트에서 한개 보고서 row를 입력 받아 재무재표 tag를 추출한다.
def searchlinkedjemu(company_code,row):
    #index_date 세팅
    pattern = re.compile(r'(.+\()(\d+\.\d+)(\))')
    result = pattern.findall(row[3])
    index_date=result[0][1]

    url = "http://dart.fss.or.kr/dsaf001/main.do?rcpNo="+row[4]
    req = requests.get(url)

    #company테이블에서 해당기업의 연결재무재표여부 정보를 가져 온다.
    con = psycopg2.connect("dbname='webcrolldb' user='postgres' host='localhost' password='1111'")
    selectsql = '''select * from company where code=%s'''
    cur = con.cursor()
    cur.execute(selectsql,(company_code,))
    rows = cur.fetchall()
    #print("rows4값이 머냐")
    #print(rows[0][4])



    #print(req.text)
    #pattern=re.compile(r'function\(\)\s\{viewDoc\(\'(\d{14})\'\,\s\'(\d{7})\'\,\s\'(\d+)\'\,\s\'(\d+)\'\,\s\'(\d+)\'') #키값을 도출하는 pattern
    #pattern = re.compile(r'재무제표.+\n.+\n.+\n.+\n.+function\(\)\s\{viewDoc\(\'(\d{14})\'\,\s\'(\d{7})\'\,\s\'(\d+)\'\,\s\'(\d+)\'\,\s\'(\d+)\'')  # 키값을 도출하는 pattern
    #pattern = re.compile(r'재무제표[^!]+function\(\)\s\{viewDoc\(\'(\d{14})\'\,\s\'(\d{7})\'\,\s\'(\d+)\'\,\s\'(\d+)\'\,\s\'(\d+)\'')  # 키값을 도출하는 pattern
    linked_yn="Y"
    print('rows is')
    print(rows)
    if rows[0][4]=="Y":
        if index_date < '2014.12':
            linked_yn="N"
        else:
            linked_yn="Y"
    else:
        linked_yn="N"
    #print("linked yn is")
    #print(linked_yn)
    #print(req.text)
    if linked_yn =="N":
        pattern = re.compile(r' 재무제표[^f]*function\(\)\s\{viewDoc\(\'(\d{14})\'\,\s\'(\d{7})\'\,\s\'(\d+)\'\,\s\'(\d+)\'\,\s\'(\d+)\'')  # 키값을 도출하는 pattern
    else:
        pattern = re.compile(r'연결재무제표[^f]*function\(\)\s\{viewDoc\(\'(\d{14})\'\,\s\'(\d{7})\'\,\s\'(\d+)\'\,\s\'(\d+)\'\,\s\'(\d+)\'')  # 키값을 도출하는 pattern

    #if index_date < '2014.12':
    #    pattern = re.compile(r'재무제표[^f]*function\(\)\s\{viewDoc\(\'(\d{14})\'\,\s\'(\d{7})\'\,\s\'(\d+)\'\,\s\'(\d+)\'\,\s\'(\d+)\'')  # 키값을 도출하는 pattern
    #else:
    #    pattern = re.compile(r'연결재무제표[^f]*function\(\)\s\{viewDoc\(\'(\d{14})\'\,\s\'(\d{7})\'\,\s\'(\d+)\'\,\s\'(\d+)\'\,\s\'(\d+)\'')  # 키값을 도출하는 pattern

    #print("pattern is")
    #print(pattern)
    result = pattern.findall(req.text)
    print("result is")
    print(result)
    #if result != None:
    #    print(result[0])
    url_parsing="http://dart.fss.or.kr/report/viewer.do?rcpNo="+result[0][0]+"&dcmNo="+result[0][1]+"&eleId="+result[0][2]+"&offset="+result[0][3]+"&length="+result[0][4]+"&dtd=dart3.xsd"
    #webbrowser.open(url_parsing)
    print(url_parsing)
    print("open전 : %s seconds ---" % (time.time() - start_time))
    report=urlopen(url_parsing)
    r=report.read()
    print("open후 : %s seconds ---" % (time.time() - start_time))
    xmlsoup=BeautifulSoup(r,'html.parser')
    tags = xmlsoup.select('table > tbody > tr')
    return tags,index_date

def parsingdata(tags,company_code,index_date):
    newtags=[]
    if index_date > '2010.12':
        pattern = re.compile(r'(\<p.*\>)(.+)(\<\/p\>)')
         # pattern = re.compile(r'(\<td.*\>)(.+)(\<\/td\>)')
    else:
        pattern = re.compile(r'(\<td.*\>)(.+)(\<\/td\>)')
    binkan = re.compile(r'\s+')
    nohangul = re.compile(r'[^가-힣]+')
    nonumber = re.compile(r'[^0-9]+')
    #print('parsing 전')
    #print(tags)
    for k in tags:
        xmlsoup = BeautifulSoup(str(k), 'html.parser')
        tds = xmlsoup.select('td')
        for m in range(len(tds)):
            temp = pattern.search(str(tds[m]))
            if temp:
                tds[m] = temp.group(2)
            else:
                tds[m] = 0
            #linkedjemuraw의 insert전 itemname에 해당하는 값에 대해 한글만 남도록 교정
            if m==0:
                tds[m]=re.sub(nohangul,'',str(tds[m]))
            #linkedjemuraw의 insert전 values에 해당하는 값에 대해 교정
            else:
                tds[m] = re.sub(nonumber, '', str(tds[m]))
                tds[m] = remove_letter(str(tds[m]), ',')
                if(tds[m]==""):
                    tds[m]="0"
                if (tds[m][0] == '('):
                    tds[m] = remove_letter(tds[m], '(')
                    tds[m] = remove_letter(tds[m], ')')
                    try:
                        tds[m] = -1 * int(tds[m])
                    except:
                        tds[m] = 0
        newtags.append(tds)
    return newtags

def insertlinkedjemuraw(tags,company_code,index_date):
    # if index_date > '2010.12':
    #     pattern = re.compile(r'(\<p.*\>)(.+)(\<\/p\>)')
    #     #pattern = re.compile(r'(\<td.*\>)(.+)(\<\/td\>)')
    # else:
    #     pattern = re.compile(r'(\<td.*\>)(.+)(\<\/td\>)')
    # binkan = re.compile(r'\s+')
    # nohangul = re.compile(r'[^가-힣]+')
    # nonumber = re.compile(r'[^0-9]+')

    con = psycopg2.connect("dbname='webcrolldb' user='postgres' host='localhost' password='1111'")
    cur = con.cursor()
    sql = ''' insert into linkedjemuraw values(nextval('jemuraw_id_seq'),%s,%s,%s,%s,%s,%s,'N',current_timestamp)'''
    deletesql = ''' delete from linkedjemuraw where code=%s and index_date=%s '''
    division1=0;
    division2=0;
    #print(len(tags))
    print(tags)
    cur.execute(deletesql, (company_code, index_date))
    for i in range(len(tags)):
        itemname=""
        values=0
        # print(result)
        if tags[i]:
            if tags[i][0]=="연결포괄손익계산서":
                break
        for j in range(len(tags[i])):
            if j==0:
                if(tags[i][j]=="자산"):
                    division1=1
                elif(tags[i][j]=="부채"):
                    division1=2
                elif(tags[i][j]=="자본"):
                    division1=3
                if(tags[i][j]=="유동자산"):
                    division2=1
                elif(tags[i][j]=="비유동자산"):
                    division2=2
                elif(tags[i][j]=="유동부채"):
                    division2=3
                elif (tags[i][j] == "비유동부채"):
                    division2 = 4
                itemname=tags[i][j]
                #print(itemname)
            elif j==1:
                # values값 교정
                #print('values 타입')
                #print(type(tags[i][j]))
                #if(str(type(tags[i][j]))=="<class 'str'>"):
                #    tags[i][j] = tags[i][j].strip()
                #    tags[i][j] = tags[i][j].replace("원","")
                #    tags[i][j] = tags[i][j].replace("(", "")
                #    tags[i][j] = tags[i][j].replace(")", "")
                #    tags[i][j] = tags[i][j].replace("<br/>", "0")
                values=tags[i][j]

        try:
            cur.execute(sql,(company_code,index_date,itemname,values,division1,division2))
        except Exception as e:
            print("fail")
            print(e)
            #pass
            #print('exception occur')
        cur.execute('commit;')
    cur.close()
    con.close()

def insertlinkedjemu(company_code,index_date):
    jemuval = [0] * 900
    con = psycopg2.connect("dbname='webcrolldb' user='postgres' host='localhost' password='1111'")
    selectsql = "select * from linkedjemuraw where code=%s and index_date=%s and transferyn='N'"
    cur = con.cursor()
    cur.execute(selectsql,(company_code,index_date))
    rows = cur.fetchall()
    index_num = 0
    jemuraw_id=[]
    sql = ''' insert into linkedjemu values(nextval('jemu_id_seq'),%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,current_timestamp,%s,%s,%s,%s,%s,%s,%s,%s
            )'''
    updatesql = "update linkedjemuraw set transferyn='Y' where jemuraw_id = ANY(%s)"
    itemrows=searchitem()
    for row in rows:
        selecteditemid = 10000
        for itemrow in itemrows:
            for i in range(len(itemrow)):
                if (row[3] == itemrow[i]):
                    selecteditemid = itemrow[0]
        #print('selecteditemid')
        #print(row[3])
        #print(selecteditemid)
        if (selecteditemid == 220):  # 만기보유금융자산
            if row[6] == 1:
                selecteditemid = 22
            else:
                selecteditemid = 85
        if (selecteditemid == 221):  # 충당부채
            if row[6] == 3:
                selecteditemid = 139
            else:
                selecteditemid = 174
        if (selecteditemid == 222):  # 기타투자자산2
            selecteditemid = 220
        if (selecteditemid == 18):  # 단기손익인식금융자산
            if row[6] == 1:
                selecteditemid = 18
            else:
                selecteditemid = 87
        if (selecteditemid == 223):  #차입금
            if row[6] == 3:
                selecteditemid = 111
            else:
                selecteditemid = 157
        if (selecteditemid in (224,1003)):  #사채및차입금 #단기차입금및유동성장기차입금
            if row[6] == 3:
                selecteditemid = 111
            else:
                selecteditemid = 157

        ##조건의 id는 checkitem번호이고, 실제 할당되는 id는 linkedjemu 컬럼번호-1
        if (selecteditemid in (225,226,227,1001,1004)):  #당기손익공정가치금융자산 #당기손익공정가치측정금융자산 #단기투자자산
            if row[6] == 1:                #당기손익공정가치_유동 이 신규컬럼이다 보니 selecteditemid를 지정해줘야함
                selecteditemid = 221
            else:
                selecteditemid = 222

        if (selecteditemid in (228, 229,230)):  # 상각후원가금융자산
            if row[6] == 1:
                selecteditemid = 223
            else:
                selecteditemid = 224

        if (selecteditemid == 1002):  #리스부채
            if row[6] == 3:
                selecteditemid = 124
            else:
                selecteditemid = 161

        if (selecteditemid == 1005):  #"당기손익공정가치금융부채주"
            if row[6] == 3:
                selecteditemid = 225
            else:
                selecteditemid = 226
        if (selecteditemid == 1006):  #"기타포괄손익공정가치금융자산주"
            if row[6] == 1:
                selecteditemid = 227
            else:
                selecteditemid = 228

        if (selecteditemid == 86):  #기타금융자산 : 기타금융자산은 비유동자산의 투자자산에 속하는데 유동자산에 있는 경우는 기타포괄손익공정가치측정금융자산_유동 으로 할당해버림
            if row[6] == 1:
                selecteditemid = 227
            else:
                selecteditemid = 86

        index_num = selecteditemid-1

        # 자산, 부채는 checktable에 없지만 그냥 transferyn을 y로 업데이트한다 row[3]이 itemname, row[4]가 값
        print(row)
        if row[3] in ('자산','부채'):
            jemuraw_id.append(row[0])

        if (index_num != 9999):
            jemuval[index_num] = row[4]
            jemuraw_id.append(row[0])
    cur.execute(updatesql, (jemuraw_id,))
    cur.execute('commit;')
    #print('cccccccccccccccc')
    jemuval = adjustvalue(jemuval) # 최종 값 보정
    #print('dddddddddddddddd')
    cur.execute(sql, (
    company_code,index_date, jemuval[0], jemuval[1], jemuval[2], jemuval[3], jemuval[4], jemuval[5], jemuval[6], jemuval[7],
    jemuval[8], jemuval[9], jemuval[10], jemuval[11], jemuval[12], jemuval[13], jemuval[14], jemuval[15], jemuval[16],
    jemuval[17], jemuval[18], jemuval[19], jemuval[20], jemuval[21], jemuval[22], jemuval[23], jemuval[24], jemuval[25],
    jemuval[26], jemuval[27], jemuval[28], jemuval[29], jemuval[30], jemuval[31], jemuval[32], jemuval[33], jemuval[34],
    jemuval[35], jemuval[36], jemuval[37], jemuval[38], jemuval[39], jemuval[40], jemuval[41], jemuval[42], jemuval[43],
    jemuval[44], jemuval[45], jemuval[46], jemuval[47], jemuval[48], jemuval[49], jemuval[50], jemuval[51], jemuval[52],
    jemuval[53], jemuval[54], jemuval[55], jemuval[56], jemuval[57], jemuval[58], jemuval[59], jemuval[60], jemuval[61],
    jemuval[62], jemuval[63], jemuval[64], jemuval[65], jemuval[66], jemuval[67], jemuval[68], jemuval[69], jemuval[70],
    jemuval[71], jemuval[72], jemuval[73], jemuval[74], jemuval[75], jemuval[76], jemuval[77], jemuval[78], jemuval[79],
    jemuval[80], jemuval[81], jemuval[82], jemuval[83], jemuval[84], jemuval[85], jemuval[86], jemuval[87], jemuval[88],
    jemuval[89], jemuval[90], jemuval[91], jemuval[92], jemuval[93], jemuval[94], jemuval[95], jemuval[96], jemuval[97],
    jemuval[98], jemuval[99], jemuval[100], jemuval[101], jemuval[102], jemuval[103], jemuval[104], jemuval[105],
    jemuval[106], jemuval[107], jemuval[108], jemuval[109], jemuval[110], jemuval[111], jemuval[112], jemuval[113],
    jemuval[114], jemuval[115], jemuval[116], jemuval[117], jemuval[118], jemuval[119], jemuval[120], jemuval[121],
    jemuval[122], jemuval[123], jemuval[124], jemuval[125], jemuval[126], jemuval[127], jemuval[128], jemuval[129],
    jemuval[130], jemuval[131], jemuval[132], jemuval[133], jemuval[134], jemuval[135], jemuval[136], jemuval[137],
    jemuval[138], jemuval[139], jemuval[140], jemuval[141], jemuval[142], jemuval[143], jemuval[144], jemuval[145],
    jemuval[146], jemuval[147], jemuval[148], jemuval[149], jemuval[150], jemuval[151], jemuval[152], jemuval[153],
    jemuval[154], jemuval[155], jemuval[156], jemuval[157], jemuval[158], jemuval[159], jemuval[160], jemuval[161],
    jemuval[162], jemuval[163], jemuval[164], jemuval[165], jemuval[166], jemuval[167], jemuval[168], jemuval[169],
    jemuval[170], jemuval[171], jemuval[172], jemuval[173], jemuval[174], jemuval[175], jemuval[176], jemuval[177],
    jemuval[178], jemuval[179], jemuval[180], jemuval[181], jemuval[182], jemuval[183], jemuval[184], jemuval[185],
    jemuval[186], jemuval[187], jemuval[188], jemuval[189], jemuval[190], jemuval[191], jemuval[192], jemuval[193],
    jemuval[194], jemuval[195], jemuval[196], jemuval[197], jemuval[198], jemuval[199], jemuval[200], jemuval[201],
    jemuval[202], jemuval[203], jemuval[204], jemuval[205], jemuval[206], jemuval[207], jemuval[208], jemuval[209],
    jemuval[210], jemuval[211], jemuval[212], jemuval[213], jemuval[214], jemuval[215], jemuval[216], jemuval[217],
    jemuval[218],jemuval[219],jemuval[220],jemuval[221],jemuval[222],jemuval[223],jemuval[224],jemuval[225],jemuval[226],jemuval[227]))
    cur.execute('commit;')
    print('eeeeeeeeeeeeeeeeee')
    cur.close()
    con.close()

def adjustvalue(jemuval):
    if jemuval[85]<1: #기타금융자산이 0이면 세부항목을 더할 것
        jemuval[85]=jemuval[86]+jemuval[87]+jemuval[88]+jemuval[89]+jemuval[90]+jemuval[219]
    if jemuval[78]<1: #투자자산이 0이면 세부항목을 더할 것
        jemuval[78]=jemuval[79]+jemuval[83]+jemuval[84]+jemuval[85]
    if jemuval[111]<1: #유동성장기부채가 0이면 세부항목을 더할 것
        jemuval[111]=jemuval[112]+jemuval[113]+jemuval[114]+jemuval[115]+jemuval[116]+jemuval[117]+jemuval[118]
    if jemuval[15] >0 and (jemuval[16]+jemuval[17]+jemuval[18]+jemuval[20]+jemuval[21])==0:  # 재무재표가 단기금융자산 항목으로만 들어온 경우 순부채 계산을 위해 단기금융상품에 해당 값을 할당
        jemuval[18] = jemuval[15]

    return jemuval


def companylist():  # itemname으로 checkitem테이블을 뒤져서 index반환
    con = psycopg2.connect("dbname='webcrolldb' user='postgres' host='localhost' password='1111'")
    selectsql = " select companyid,code,companyname from company where enable='Y' and usunjuyn='N' order by companyid; "
    cur = con.cursor()
    cur.execute(selectsql)
    companyrows = cur.fetchall()

    cur.close()
    con.close()
    return companyrows

def naverjemu():

    url = "http://dart.fss.or.kr/api/search.xml?auth="+auth_key+"&crp_cd="+company_code+"&start_dt="+start_date+"&end_dt="+end_date+"&bsn_tp=A001&bsn_tp=A002&bsn_tp=A003"

    print(url);
    resultXML=urlopen(url) #this is for response of XML
    result=resultXML.read() #Using read method
    xmlsoup=BeautifulSoup(result,'html.parser')

    data = pd.DataFrame()
    te=xmlsoup.findAll("list")
    for t in te:
        temp=pd.DataFrame(([[t.crp_cls.string,t.crp_nm.string,t.crp_cd.string,t.rpt_nm.string, t.rcp_no.string,t.flr_nm.string,t.rcp_dt.string, t.rmk.string]]), columns=["crp_cls","crp_nm","crp_cd","rpt_nm","rcp_no","flr_nm","rcp_dt","rmk"])
        data=pd.concat([data,temp])
    data=data.reset_index(drop=True)
    #print(data.iloc[:,[1,3,4,5,6,7]])
    #print('data type')
    #print(type(data))
    if len(data) > 0: #dataframe의 null체크를 len()함수로 수행함
        data=data[data.rmk!='정연'] #기재정정이 발생하면 기 보고서의 rmk컬럼에 "정"값이 들어가기 때문에 해당 보고서를 list에서 제외하는 로직 추가
        data =data[data.rmk != '정']
        data = data.reset_index(drop=True) # dataframe에서 행을 제거하면 해당 행이 null로 남기 때문에 reset_index로 해당 행을 제거

    if len(data) > 0:
        data['removeyn'] = data['rpt_nm'].str.find(sub='첨부정정')  # dataframe의 리포트 이름에 첨부정정이 있으면 제거
        data = data[data.removeyn < 0]
        data = data.reset_index(drop=True)  # dataframe에서 행을 제거하면 해당 행이 null로 남기 때문에 reset_index로 해당 행을 제거
        data['removeyn'] = data['rpt_nm'].str.find(sub='연장신고서')  # dataframe의 리포트 이름에 첨부정정이 있으면 제거
        data = data[data.removeyn < 0]
        data = data.reset_index(drop=True)  # dataframe에서 행을 제거하면 해당 행이 null로 남기 때문에 reset_index로 해당 행을 제거
    return data

start_time = time.time()
print("start : %s seconds ---" %(time.time() - start_time))
searchdate=pd.DataFrame(([["20190101", "20200101"],["20170101", "20190101"],["20150101", "20170101"],["20130101", "20150101"],["20110101", "20130101"],["20100101", "20110101"]]), columns=["col1","col2"])
#searchdate=pd.DataFrame(([["20100101", "20110101"]]), columns=["col1","col2"])
#company_code = "005930"  # company code
#company_code = "000660"  # company code
#company_code = "207940"  # company code
#company_code = "035420"  # company code
#company_code = "005380"  # company code
#company_code = "068270"  # company code
startcompany_code = "005930"  # company code
beginflag=0

companylist=companylist()
for k in range(len(companylist)):
    print('company is ')
    print(companylist[k][0])
    if(companylist[k][1]==startcompany_code):
        beginflag=1
    print(beginflag)
    if(beginflag==1):
        for j in range(len(searchdate)):
            data = searchreportlist(companylist[k][1],searchdate.loc[j][0],searchdate.loc[j][1]) #data는 리포트 리스트
            if len(data) >0:
                print("searchreportlist : %s seconds ---" % (time.time() - start_time))
                for i in range(len(data)):
                    print(data.iloc[:,1:10])
                    (tags,index_date)=searchlinkedjemu(companylist[k][1],data.loc[i])
                    print("searchlinkedjemu : %s seconds ---" % (time.time() - start_time))
                    print(index_date)
                    #print(tags,companylist[k][1],index_date)
                    #print("tag raw is")
                    #print(tags)

                    # newtags = parsingdata(tags, companylist[k][0], index_date)
                    # print("parsingdata : %s seconds ---" % (time.time() - start_time))
                    # #print("tag is")
                    # print(newtags)
                    #
                    # insertlinkedjemuraw(newtags,companylist[k][1],index_date)
                    # print("insertlinkedjemuraw : %s seconds ---" % (time.time() - start_time))


                    insertlinkedjemu(companylist[k][1],index_date)
                    print("insertlinkedjemu : %s seconds ---" % (time.time() - start_time))







