use warehouse analyst_wh;
use database graphpad_db;
use schema dbt_olha;
create file format if not exists xml_file_format type = 'XML'  IGNORE_UTF8_ERRORS = TRUE;
create stage if not exists data_integrity_checks_xml_stage file_format = xml_file_format;

--put file://C:\Users\Olga\Documents\checks\check_orders_full_table\run_xml\&{file_name} @data_integrity_checks_xml_stage auto_compress=true;
put file://&{path_to_file}/&{file_name} @data_integrity_checks_xml_stage auto_compress=true;


delete from ORDERS_RAW_DATA where 
orderDate < date_trunc('day', dateadd('day',1,to_date('&{rundate}'))) and orderDate >= date_trunc('day', to_date('&{rundate}'));

CREATE OR REPLACE temp TABLE xml_orders(src VARIANT);

COPY INTO xml_orders
FROM @data_integrity_checks_xml_stage
FILE_FORMAT=(format_name = xml_file_format) ON_ERROR='CONTINUE';

remove @data_integrity_checks_xml_stage pattern='.*.xml.gz';

insert into ORDERS_RAW_DATA
SELECT
	XMLGET( VALUE, 'LOADED_AT' ):"$"::TIMESTAMP_NTZ AS LOADED_AT,
	XMLGET( VALUE, 'ORDERNUMBER' ):"$"::number AS ORDERNUMBER,
	XMLGET( VALUE, 'ORDERDATE' ):"$"::TIMESTAMP_NTZ AS ORDERDATE,
	XMLGET( VALUE, 'SHIPDATE' ):"$"::TIMESTAMP_NTZ AS SHIPDATE,
	XMLGET( VALUE, 'FIRSTNAME' ):"$"::varchar AS FIRSTNAME,	
	XMLGET( VALUE, 'LASTNAME' ):"$"::varchar AS LASTNAME,	
	XMLGET( VALUE, 'COMPANY' ):"$"::varchar AS COMPANY,	
	XMLGET( VALUE, 'ADDRESS1' ):"$"::varchar AS ADDRESS1,	
	XMLGET( VALUE, 'ADDRESS2' ):"$"::varchar AS ADDRESS2,	
	XMLGET( VALUE, 'CITY' ):"$"::varchar AS CITY,	
	XMLGET( VALUE, 'STATE' ):"$"::varchar AS STATE,
	XMLGET( VALUE, 'ZIP' ):"$"::varchar AS ZIP,	
	XMLGET( VALUE, 'COUNTRY' ):"$"::varchar AS COUNTRY,	
	XMLGET( VALUE, 'PHONE' ):"$"::varchar AS PHONE,	
	XMLGET( VALUE, 'FAX' ):"$"::varchar AS FAX,	
	XMLGET( VALUE, 'EMAIL' ):"$"::varchar AS EMAIL,	
	XMLGET( VALUE, 'CCTYPE' ):"$"::varchar AS CCTYPE,
	XMLGET( VALUE, 'CCNAME' ):"$"::varchar AS CCNAME,	
	XMLGET( VALUE, 'CCNUMBER' ):"$"::varchar AS CCNUMBER,	
	XMLGET( VALUE, 'EXPDATE' ):"$"::varchar AS EXPDATE,	
	XMLGET( VALUE, 'AUTHCODE' ):"$"::varchar AS AUTHCODE,	
	XMLGET( VALUE, 'TOTALCHARGED' ):"$"::NUMBER(38,6) AS TOTALCHARGED,	
	TO_BOOLEAN(XMLGET( VALUE, 'ISSERIALNUMBERGENERATIONPENDING' ):"$"::numeric) AS ISSERIALNUMBERGENERATIONPENDING,	
	TO_BOOLEAN(XMLGET( VALUE, 'ISSERIALNUMBERGENERATIONENABLED' ):"$"::numeric) AS ISSERIALNUMBERGENERATIONENABLED,	
	XMLGET( VALUE, 'CYBERCASHID' ):"$"::varchar AS CYBERCASHID,	
	XMLGET( VALUE, 'STATUS' ):"$"::varchar AS STATUS,	
	XMLGET( VALUE, 'S_COMPANY' ):"$"::varchar AS S_COMPANY,	
	XMLGET( VALUE, 'S_ADDRESS1' ):"$"::varchar AS S_ADDRESS1,	
	XMLGET( VALUE, 'S_ADDRESS2' ):"$"::varchar AS S_ADDRESS2,	
	XMLGET( VALUE, 'S_CITY' ):"$"::varchar AS S_CITY,	
	XMLGET( VALUE, 'S_STATE' ):"$"::varchar AS S_STATE,	
	XMLGET( VALUE, 'S_ZIP' ):"$"::varchar AS S_ZIP,	
	XMLGET( VALUE, 'S_COUNTRYID' ):"$"::number AS S_COUNTRYID,	
    XMLGET( VALUE, 'S_PHONE' ):"$"::varchar AS S_PHONE,	
	XMLGET( VALUE, 'SHIPPINGCHARGES' ):"$"::NUMBER(38,6) AS SHIPPINGCHARGES,	
	XMLGET( VALUE, 'WEIGHT' ):"$"::NUMBER(38,6) AS WEIGHT,	
	XMLGET( VALUE, 'CUSTOMERID' ):"$"::number AS CUSTOMERID,	
	XMLGET( VALUE, 'COUNTRYID' ):"$"::number AS COUNTRYID,
	XMLGET( VALUE, 'ADDRESS3' ):"$"::varchar AS ADDRESS3,	
	XMLGET( VALUE, 'S_ADDRESS3' ):"$"::varchar AS S_ADDRESS3,	
	XMLGET( VALUE, 'ORDERTYPEID' ):"$"::number AS ORDERTYPEID,	
	XMLGET( VALUE, 'SALESTAXRATE' ):"$"::NUMBER(38,6) AS SALESTAXRATE,	
	XMLGET( VALUE, 'SALESTAXAMOUNT' ):"$"::NUMBER(38,6) AS SALESTAXAMOUNT,
	XMLGET( VALUE, 'BALANCE' ):"$"::NUMBER(38,6) AS BALANCE,
	XMLGET( VALUE, 'PO_NUMBER' ):"$"::varchar AS PO_NUMBER,
	XMLGET( VALUE, 'SHIPPINGTYPEID' ):"$"::number AS SHIPPINGTYPEID,
	TO_BOOLEAN(XMLGET( VALUE, 'ISRESIDENTIAL' ):"$"::numeric) AS ISRESIDENTIAL,
	XMLGET( VALUE, 'SHIPPINGNOTES' ):"$"::varchar AS SHIPPINGNOTES,
	XMLGET( VALUE, 'S_ATTN' ):"$"::varchar AS S_ATTN,
	XMLGET( VALUE, 'ATTN' ):"$"::varchar AS ATTN,
	XMLGET( VALUE, 'SHIPPINGBATCHID' ):"$"::number AS ASSHIPPINGBATCHID,
	XMLGET( VALUE, 'S_COMPANYID' ):"$"::number AS S_COMPANYID,
	XMLGET( VALUE, 'COMPANYID' ):"$"::number AS COMPANYID,
	XMLGET( VALUE, 'INVOICENUMBER' ):"$"::varchar AS INVOICENUMBER,
	XMLGET( VALUE, 'CREATEDBY' ):"$"::varchar AS CREATEDBY,
	XMLGET( VALUE, 'ORDEREDBY' ):"$"::varchar AS ORDEREDBY,
	TO_BOOLEAN(XMLGET( VALUE, 'ISADVANCEDNETWORKORDER' ):"$"::numeric) AS ISADVANCEDNETWORKORDER,
	XMLGET( VALUE, 'RETURNTYPEID' ):"$"::number AS RETURNTYPEID,
	XMLGET( VALUE, 'STATEMENTID' ):"$"::number AS STATEMENTID,
	XMLGET( VALUE, 'S_COUNTY' ):"$"::varchar AS S_COUNTY,
	XMLGET( VALUE, 'ORIG_ORDERNUMBER' ):"$"::number AS ORIG_ORDERNUMBER,
	XMLGET( VALUE, 'DOWNLOADNOTIFICATIONEMAIL' ):"$"::varchar AS DOWNLOADNOTIFICATIONEMAIL,
	TO_BOOLEAN(XMLGET( VALUE, 'ISPRINTINVOICEENABLED' ):"$"::numeric) AS ISPRINTINVOICEENABLED,
	XMLGET( VALUE, 'SHIPPINGPRIORITY' ):"$"::number AS SHIPPINGPRIORITY,
	TO_BOOLEAN(XMLGET( VALUE, 'ISSINGLECOPY' ):"$"::numeric) AS ISSINGLECOPY,
	XMLGET( VALUE, 'PLATFORMTYPES' ):"$"::varchar AS PLATFORMTYPES,
	TO_BOOLEAN(XMLGET( VALUE, 'ISFREEUPGRADEORDER' ):"$"::numeric) AS ISFREEUPGRADEORDER,
	XMLGET( VALUE, 'B_EMAIL' ):"$"::varchar AS B_EMAIL,
	XMLGET( VALUE, 'S_PO_NUMBER' ):"$"::varchar AS S_PO_NUMBER,
	TO_BOOLEAN(XMLGET( VALUE, 'ISINVOICEPRINTED' ):"$"::numeric) AS ISINVOICEPRINTED,
	XMLGET( VALUE, 'RENEWEDSUBSCRIPTIONID' ):"$"::number AS RENEWEDSUBSCRIPTIONID,
	XMLGET( VALUE, 'SUBSCRIPTIONID' ):"$"::number AS SUBSCRIPTIONID,
	XMLGET( VALUE, 'EXTERNALINVOICENUMBER' ):"$"::varchar AS EXTERNALINVOICENUMBER,
	XMLGET( VALUE, 'ORDERCATEGOTYID' ):"$"::number AS ORDERCATEGOTYID,
	XMLGET( VALUE, 'DATECREATED' ):"$"::TIMESTAMP_NTZ AS DATECREATED,
	XMLGET( VALUE, 'DATEMODIFIED' ):"$"::TIMESTAMP_NTZ AS DATEMODIFIED
FROM xml_orders,
LATERAL FLATTEN( INPUT => SRC:"$" );

delete from data_integrity_checks where 
reporting_date < date_trunc('day', dateadd('day',1,to_date('&{rundate}'))) and reporting_date >= date_trunc('day', to_date('&{rundate}'));

create temporary table TEMP_RAW_ORDERS_DATA as 
select * 
from graphpad_db.dbt_olha.orders_raw_data
where orderDate::timestamp_ntz < date_trunc('day', dateadd('day',1,to_date('&{rundate}'))) and orderDate::timestamp_ntz >= date_trunc('day', to_date('&{rundate}'));

create temporary table TEMP_STITCH_ORDERS_DATA as 
select * 
from graphpad_db.public.orders
where orderDate::timestamp_ntz < date_trunc('day', dateadd('day',1,to_date('&{rundate}'))) and orderDate::timestamp_ntz >= date_trunc('day', to_date('&{rundate}'));


--Total Count
insert into data_integrity_checks
select 
	current_timestamp as created_at,
	date_trunc('day', to_timestamp('&{rundate}'))  as reporting_date,
	'orders' as table_name, 
	'Stitch' as source,
	null as row_identifier,
	'Total Count' as metric_name,
	totals.stitch_total_count as current_value,
	totals.raw_total_count as correct_value,
	'Total rows count does not match' as description
from (
	select 
	count(temp_raw_orders_data.orderNumber) as raw_total_count,
	count(temp_stitch_orders_data.orderNumber) as stitch_total_count
	from temp_raw_orders_data
	full join temp_stitch_orders_data on temp_raw_orders_data.orderNumber = temp_stitch_orders_data.orderNumber
	) as totals
where totals.stitch_total_count != totals.raw_total_count;

--Missed rows
insert into data_integrity_checks
with totals as (
	select 
	r.orderNumber as r_orderNumber,
	s.orderNumber as s_orderNumber
	from temp_raw_orders_data as r
	full join temp_stitch_orders_data as s on r.orderNumber = s.orderNumber
	where r.orderNumber is null or s.orderNumber is null
	)
	
select 
	current_timestamp as created_at,
	date_trunc('day', to_timestamp('&{rundate}'))  as reporting_date,
	'orders' as table_name, 
	'Stitch' as source,
	r_orderNumber as row_identifier,
	'Missed rows' as metric_name,
	NULL as current_value,
	NULL as correct_value,
	'Row missed in source' as description
from totals
where s_orderNumber is null

union all
select 
	current_timestamp as created_at,
	date_trunc('day', to_timestamp('&{rundate}'))  as reporting_date,
	'orders' as table_name, 
	'Stitch' as source,
	s_orderNumber as row_identifier,
	'Extra rows' as metric_name,
	NULL as current_value,
	NULL as correct_value,
	'Extra row in source' as description
from totals
where r_orderNumber is null;


-- checksum
insert into data_integrity_checks
select 
	current_timestamp as created_at,
	date_trunc('day', to_timestamp('&{rundate}')) as reporting_date,
	'orders' as table_name, 
	'Stitch' as "source",
	totals.orderNumber as row_identifier,
	'Checksum' as metric_name,
	totals.stitch_checksum as current_value,
	totals.raw_checksum as correct_value,
	'Checksum for orderDate, status, totalCharged, balance does not match (MD5 string)' as description
from (
	select 
    coalesce(r.orderNumber,s.orderNumber) as orderNumber,
	MD5( concat(r.orderDate::timestamp_ntz, r.status, r.totalCharged, r.balance)) as raw_checksum,
	MD5( concat(s.orderDate::timestamp_ntz, s.status, s.totalCharged, s.balance)) as stitch_checksum
	from temp_raw_orders_data as r
	join temp_stitch_orders_data as s on r.orderNumber = s.orderNumber::number
	) as totals
where totals.raw_checksum != totals.stitch_checksum;

--Status aggreagations

insert into data_integrity_checks
with status_totals as (
	select 
	coalesce(r.status, s.status) as status,
	count(r.orderNumber) as raw_total_count,
	count(s.orderNumber) as stitch_total_count,
	sum(r.totalCharged) as raw_total_charged,
	sum(s.totalCharged) as stitch_total_charged,
	sum(r.balance) as raw_balance,
	sum(s.balance) as stitch_balance
	from temp_raw_orders_data as r
	full join temp_stitch_orders_data as s on r.orderNumber = s.orderNumber
	group by coalesce(r.status, s.status)
	)
	

select 
	current_timestamp as created_at,
	date_trunc('day', to_timestamp('&{rundate}')) as reporting_date,
	'orders' as table_name, 
	'Stitch' as source,
	null as row_identifier,
	concat(status, ' orders ', 'Total Count') as metric_name,
	status_totals.stitch_total_count as current_value,
	status_totals.raw_total_count as correct_value,
	concat('Total rows count does not match for ', status, ' status') as description
from status_totals
where status_totals.raw_total_count != status_totals.stitch_total_count
union all
select 
	current_timestamp as created_at,
	date_trunc('day', to_timestamp('&{rundate}')) as reporting_date,
	'orders' as table_name, 
	'Stitch' as source,
	null as row_identifier,
	concat(status, ' orders ', 'Total Charged') as metric_name,
	status_totals.stitch_total_charged as current_value,
	status_totals.raw_total_charged as correct_value,
	concat('Total charged does not match for ', status, ' status') as description
from status_totals
where status_totals.raw_total_charged != status_totals.stitch_total_charged
union all
select 
	current_timestamp as created_at,
	date_trunc('day', to_timestamp('&{rundate}'))  as reporting_date,
	'orders' as table_name, 
	'Stitch' as source,
	null as row_identifier,
	concat(status, 'orders ', 'Total Balance') as metric_name,
	status_totals.stitch_balance as current_value,
	status_totals.raw_balance as correct_value,
	concat('Total balance does not match for ',status, ' status') as description
from status_totals
where status_totals.raw_balance != status_totals.stitch_balance;



