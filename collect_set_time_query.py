# --*-- coding: UTF-8 --*--
import subprocess
import datetime
import MySQLdb
import os
import getpass
import sys

def find_slowlog():
    mysql_cmd = '/usr/local/mysql/bin/mysql --user=%s --password=%s --host=%s -B -e "show global variables like \'slow_query_log_file\';" | grep \'slow\' | awk \'{print $2}\' ' % (LOCAL_DB_USER,LOCAL_DB_PASSWD,LOCAL_DB_HOST)
    child = subprocess.Popen(mysql_cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    outData, errData = child.communicate()
    child.wait()
    SLOWLOGPOS = outData.split('\n')[0]
    return SLOWLOGPOS

def conn_mysql(host,user,password,db):
    try:
        conn = MySQLdb.connect(host=host, user=user, passwd=password, db=db, port=3306, charset='utf8');
    except MySQLdb.Error, e:
        print "MySQL Error:%s" % str(e)
    return conn


if __name__=='__main__':
    ## 检查用户
    if getpass.getuser() != "dbMon" and  getpass.getuser() != "root":
        print "This script only can run by dbMon or root user"
        sys.exit(1)

    LOCAL_DB_USER = 'xxxx'
    LOCAL_DB_PASSWD = 'xxxxx'
    LOCAL_DB_HOST = 'xxxxx'
    LOCAL_DB = 'xxxxx'

    QUERY_REVIEW = 'get_long_query'
    QUERY_REVIEW_HISTORY = 'get_long_query_history'

    ## 判断参数个数
    if len(sys.argv)  <  2:
        print "USAGE1: %s %s" % (sys.argv[0],"max_time")
        print "USAGE2: %s %s %s" % (sys.argv[0],"max_time","slowlogfile")
        print "EXAMPLE USAGE1: %s 5 " % sys.argv[0]
        print "EXAMPLE USAGE2: %s 5 /data/dbdata/mysqllog/slow-query.log " % sys.argv[0]
        sys.exit()

    # 如果/tmp/long_time_query.csv文件存在，则退出
    data_file='/tmp/long_time_query.csv'
    if os.path.isfile(data_file):
        print "%s already exist" % data_file
        sys.exit()


    #如果用户输入慢日志则解析用户输的日志，否则解析当前mysql的慢日志文件
    if len(sys.argv)  ==  2:
        slowlogfile = find_slowlog()
    else:
        inputfile = sys.argv[2]
        if not (os.path.isfile(inputfile)):
            print "slowlog file is not exist"
            sys.exit()
        else:
            slowlogfile=inputfile

    #解析慢日志
    max_time = sys.argv[1]
    pt_cmd = 'pt-query-digest %s --since 240h  --limit 300  --order-by Query_time:max --review h=%s,D=%s,t=%s,u=%s,p=%s --history h=%s,D=%s,t=%s,u=%s,p=%s ' % (slowlogfile,LOCAL_DB_HOST, LOCAL_DB,QUERY_REVIEW, LOCAL_DB_USER,LOCAL_DB_PASSWD, LOCAL_DB_HOST, LOCAL_DB,QUERY_REVIEW_HISTORY, LOCAL_DB_USER, LOCAL_DB_PASSWD)
    child = subprocess.Popen(pt_cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    outData, errData = child.communicate()
    if (child.returncode != 0):
        print "execute %s command have problems!" % pt_cmd
    else:
        print "success execute pt-query-digest command!"

    #数据导出到/tmp/long_time_query.csv文件中
    local_dbh = conn_mysql(LOCAL_DB_HOST, LOCAL_DB_USER, LOCAL_DB_PASSWD, LOCAL_DB)
    local_cursor = local_dbh.cursor()
    data_dump_sql="""select  Query_time_max,Query_time_pct_95,sample  from get_long_query_history where Query_time_max >= %s order by Query_time_max  desc INTO OUTFILE "%s" FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';""" % (max_time,data_file)
#    print data_dump_sql
    local_cursor.execute(data_dump_sql)

    try:
        drop_query_review_sql = 'drop table %s' % QUERY_REVIEW
        local_cursor.execute(drop_query_review_sql)
        #print "drop table %s successfully!" % QUERY_REVIEW
    except MySQLdb.Error, e:
        print e
        print "drop table %s failed"  % QUERY_REVIEW

    try:
        drop_query_review_history_sql = 'drop table %s' % QUERY_REVIEW_HISTORY
        local_cursor.execute(drop_query_review_history_sql)
        #print "drop table %s successfully!" % QUERY_REVIEW_HISTORY
    except MySQLdb.Error, e:
        print e
        print "drop table %s failed"  % QUERY_REVIEW_HISTORY
    finally:
        local_cursor.close()
        local_dbh.close()

    #添加表头信息
    data_file='/tmp/long_time_query.csv'
    head_info="最长执行时间,平均执行时间,sql语句\n"
    with open(data_file, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(head_info+content)
    print "please check file %s" % data_file
