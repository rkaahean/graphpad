{{
  config(
    materialized = "view",
    alias = "customer_base_table",
    tags = ["graphpad", "customer"]
  )
}}

with source as
(
select *
from {{ source('prism','customers')}}


),

sanitation as
(
select
CUSTOMERID as customer_id
, case when COMPANYID is NULL then 'Individual' else 'Company' end as customer_type
, COMPANYID as company_id

from source



)

select * from sanitation
