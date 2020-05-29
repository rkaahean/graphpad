{{
  config(
    materialized = "view",

    tags = ["brands"]
  )
}}

with source as
(
select *
from {{ source('core','brands')}}


),

sanitation as
(
select
brandCode as brand_code
,brandDomain as brand_domain
,brandID as brand_id
,brandName as brand_name
,dateCreated as date_created
,isActive as is_active

from source

)

select * from sanitation
