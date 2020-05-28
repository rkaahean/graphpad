{{
  config(
    materialized = "view",

    tags = ["prices"]
  )
}}

with source as
(
select *
from {{ source('core','prices')}}


),

sanitation as
(
  SELECT
  authCode as authcode
  ,endQty as end_qty
  ,isActive as is_active
  ,isActiveInternal as is_active_internal
  ,planID as plan_id
  ,priceID as price_id
  ,productID as product_id
  ,startQty as start_qty
  ,unitPrice as unit_price

from source

)

select * from sanitation
