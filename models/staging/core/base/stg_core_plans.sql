{{
  config(
    materialized = "view",

    tags = ["plans"]
  )
}}

with source as
(
select *
from {{ source('core','plans')}}


),

sanitation as
(
select
brandID as brand_id
,defaultAuthCode as default_auth_code
,isStandardPlan as is_standard_plan
,name as plan_name
,planCode as plan_code
,planID as plan_id
,planIntervalLength as plan_interval_length
,planIntervalUnit as plan_interval_unit
,status
,type
, case when status = 'active' then TRUE else FALSE end as is_active

from source

)

select * from sanitation
