#!/bin/bash
###############################################
## Function: rotate slow_log ,interval everyday,collected to monitor
## Version : 1.2.1
## /bin/bash /root/xa/monitor_slow_log.sh 3306 231 >> /data/dbbak/ops_mysql.log 2>&1
## 3306  -- mysql port
## 231   -- mysql id (on monitor system)
###############################################

export port=$1
export monitor_id=$2

#config monitor database server

monitor_sys_db_host="xxxxxxxx"
monitor_sys_db_port=xxxx
monitor_sys_db_user="xxxxx"
monitor_sys_db_password="xxxxxx"
monitor_sys_db_database="xxxxxx"

#config mysql server

DATE="`date "+%F"`"
DEL_DATE="`date "+%F" -d '12 days ago'`"
mysql_client="/usr/local/mysql/bin/mysql"
mysql_host="127.0.0.1"
mysql_port=${port}
mysql_user="xxxx"
mysql_password="xxxxxx"

#config slowqury

pt_query_digest="/usr/bin/pt-query-digest"

#config server_id
server_id=${monitor_id}

#collect mysql slowquery log into database

collecte_slow_log(){
   slowquery_file=`$mysql_client -h$mysql_host -P$mysql_port -u$mysql_user -p$mysql_password  -e "show variables like 'slow_query_log_file'"|grep log|awk '{print $2}'`
   $pt_query_digest --user=$monitor_sys_db_user --password=$monitor_sys_db_password --port=$monitor_sys_db_port --review h=$monitor_sys_db_host,D=$monitor_sys_db_database,t=mysql_slow_query_review  --history h=$monitor_sys_db_host,D=$monitor_sys_db_database,t=mysql_slow_query_review_history  --no-report --limit=100% --filter=" \$event->{add_column} = length(\$event->{arg}) and \$event->{serverid}=$server_id " $slowquery_file > /tmp/slowquery.log
}

#rotate mysql slowlog file

rotate_log(){
   ${mysql_client} -u${mysql_user} -p${mysql_password} -S /data/mysql/${port}/mysql.sock information_schema -sss <<EOF
set global slow_query_log=0;
select VARIABLE_VALUE into @old_slow_log_file from GLOBAL_VARIABLES where VARIABLE_NAME='SLOW_QUERY_LOG_FILE';
set global slow_query_log_file='/data/mysql/${port}/mysql_slow_${DATE}.log';
set global slow_query_log=1;
select VARIABLE_VALUE into @new_slow_log_file from GLOBAL_VARIABLES where VARIABLE_NAME='SLOW_QUERY_LOG_FILE';

select concat('Change Slow log file From ',@old_slow_log_file,' To --> ',@new_slow_log_file);
EOF
  if [ $? -eq 0 ]; then
     echo "`date "+%F %T"` : Switch Log File Success !"
  else
     echo "`date "+%F %T"` : Switch Log File Falied !"
     exit -100
  fi
}

#remove history slowlog file 

remove_log(){
   _FILE_DEL_="/data/mysql/${port}/mysql_slow_${DEL_DATE}.log"
   echo "`date "+%F %T"` : Execute command : rm -f ${_FILE_DEL_}"
   rm -f /data/mysql/${port}/mysql_slow_${DEL_DATE}.log >/dev/null 2>&1
   if [ $? -eq 0 ]; then
      echo "`date "+%F %T"` : Remove File[${_FILE_DEL_}] Success !"
   else
      echo "`date "+%F %T"` : Remove File[${_FILE_DEL_}] Failed !"
   fi
}

export port=$1
export monitor_id=$2
collecte_slow_log
rotate_log
remove_log
