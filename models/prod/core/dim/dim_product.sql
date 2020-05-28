{{
  config(
    materialized = "table",

    tags = ["products","product",'dim','dimension']
  )
}}


with product_rel as
(
SELECT distinct
product_id
, plan_id
, authcode
FROM {{ ref('stg_core_prices')}}

)

, product_brand_detail as
(
SELECT
distinct
product_id
, product_name
, product_name_abbrev
, brand_id
, brand_name
, seat_category_range

FROM
{{ ref('stg_core_product_brand_detail')}}

)

, plan_detail as
(
  select distinct
  plan_id
  ,plans.plan_code
  ,plans.plan_name
  ,plans.type
  ,plans.plan_interval_unit
  ,plans.plan_interval_length

  FROM
  {{ ref('stg_core_plans')}} plans

)

, authcode_detail as
(
SELECT distinct
authcode
, authcode_description
from
{{ref('stg_core_coupons')}}


)

, master as
(
SELECT
distinct
pb.*
, pl.*
, c.*
FROM product_rel r
inner join product_brand_detail pb on r.product_id = pb.product_id
inner join plan_detail pl on r.plan_id = pl.plan_id
inner join authcode_detail c on r.authcode = c.authcode


)

select *
from master
