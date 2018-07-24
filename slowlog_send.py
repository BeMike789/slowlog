#!/bin/env python
#-*-coding:utf-8-*-
#Funcation:send slow log to DBA
#date:2017-07-24 

import sys
import string
import datetime
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
reload(sys)
sys.setdefaultencoding('utf8')

my_sender='your_sender_email_account'
my_sender_password='your_sender_email_account_password'
to_user=sys.argv[1]+',yourreceiveaccount@xxxx'
title=sys.argv[2]
my_user=string.splitfields(to_user,",")
userfilter="'xxxx','xxxx','xxxxx'"
conf = {
    'host':'xxxxxx',
    'port':3306,
    'user':'xxxxx',
    'passwd':'xxxx',
    'db':'test',
    'charset':'utf8'
}

def getslowlog(cnt,sltime,filter,etime,logtype ='db'):
    str=('mysql+mysqldb://{username}:{password}@{host}:{port}/{database}''?charset=utf8'.format(username=conf.get('user',''),password=conf.get('passwd',''),host=conf.get('host',''),port=conf.get('port',3306),database=conf.get('db','')))
    engine=create_engine(str,echo=False)
    dsession=sessionmaker(bind=engine)
    session=dsession()
    if logtype == 'db':
        sql='''select b.db_max,b.user_max,a.first_seen,a.last_seen,b.Query_time_pct_95,sum(b.ts_cnt) count,b.sample from mysql_slow_query_review a join mysql_slow_query_review_history b on a.checksum=b.checksum 
            where b.Query_time_pct_95 > '''+ sltime+" and a.last_seen > "+"'"+etime+"'"+" and b.user_max not in ("+filter+") group by a.checksum having count >"+cnt+" order by count desc"
    else:
        sql="select a.domain,b.host,b.port,b.command,duration,start_time,a.tags from db_servers_redis a,redis_slow_query_review b where a.host=b.host and start_time > '%s' and duration > 500 group by host,port,command order by start_time " % etime
    print sql
    return session.execute(sql).fetchall()
    session.close

def createtbredis(result,s7day,s3day):
    table='''
    以下是 %s to %s 期间内耗时超过500ms的Redis操作</p>
    <table table border="1" cellspacing="0" cellpadding="2" align="center">
        <tr>
                <th>编号</th>
                <th>域名</th>
                <th>IP</th>
                <th>PORT</th>
                <th>操作</th>
                <th>耗时(ms)</th>
                <th>开始时间</th>
                <th>备注信息</th>
        </tr>
    ''' % (str(s7day),str(s3day))
    i = 1
    for qrow in result:
        table=table+"<tr>"
        table=table+"<td>"+str(i)+"</td>"
        for cloum in qrow:
            table=table+"<td>"+str(cloum)+"</td>"
        table=table+"</tr>"  
        i = i + 1
    table=table+'''
        </table>
        </body>
        </html>
        '''
    return table

def createtbdb(result,s3day):
    table = '''
    以下是 %s 至今查询大于2秒，执行次数大于100的慢查询sql</p>
    <table table border="1" cellspacing="0" cellpadding="2" align="center">
        <tr>
                <th>编号</th>
                <th>database</th>
                <th>user</th>
                <th>首次出现时间</th>
                <th>最近出现时间</th>
                <th>95%%执行时间</th>
                <th>执行次数</th>
                <th>sql</th>
                <th>备注</th>
        </tr> 
    ''' % (str(s3day))

    i = 1
    for qrow in result:
        table=table+"<tr>"
        table=table+"<td>"+str(i)+"</td>"
        for cloum in qrow:
            table=table+"<td>"+str(cloum)+"</td>"
        table=table+"</tr>\n"
        i = i + 1
    table=table+'''
        </table>
        <p>'''
    return table

def mail(html):
    ret=True
    try:
        msg=MIMEText(html,'html','utf-8')
        msg['From']=formataddr(["慢日志查询",my_sender])
        msg['To']=formataddr(["邮件昵称",my_user])
        msg['Subject']=title
        server=smtplib.SMTP()
        server.connect('smtp.exmail.qq.com')
        server.login(my_sender,my_sender_password)
        server.sendmail(my_sender,my_user,msg.as_string())
        server.quit()
    except Exception,e:
        print Exception,":",e
        ret=False

    return ret

def main():
    html='''
    <!DOCTYPE html>
    <html>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <head>
    <title>数据库慢查询</title>
    </head>
    <body>
    <p>Hi ALL:<br>'''
    today = datetime.date.today()
    s7day = today - datetime.timedelta(days=1)
    s3day = today - datetime.timedelta(days=1)
    result1=getslowlog('100','2',userfilter,str(s3day))
    if result1:
        table1=createtbdb(result1,s3day)
    else:
        table1=''
    result2=getslowlog('100','2',str(s7day),str(s3day),logtype='redis')
    if result2:
        table2=createtbredis(result2,s7day,s3day)
    else:
        table2='</body></html>'
    if len(result1) == 0 and len(result2) ==0:
        html=html+'<br> 昨天未发现慢日志。</p>'+table2
    else:
        html=html+table1+table2
    ret=mail(html)
    if ret:
        print("ok")
    else:
        print("failed")

if __name__ == "__main__":
    main()
