today=`date '+%Y%m%d%H%M'`
if test -z "$1"
then 
	reporting_date=$(date '+%Y-%m-%d')		
else
	reporting_date=$1
fi
cur_dir=`pwd`
file_export=${today}_orders_raw_data.xml

sqlcmd -S rds.graphpad.com -U graphpad_readOnly -P ******** -d graphpad -v rundate=${reporting_date} -i "mssql_query.sql" -o "${file_export}" -y0
snowsql  -c checks -f snowsql_query.sql --variable file_name=${file_export} --variable rundate=${reporting_date} --variable path_to_file=${cur_dir} | tee ${cur_dir}/logs/${today}_orders_raw_data_logs.txt
rm ${file_export}
read -p 'Press [Enter] key to continue...'
