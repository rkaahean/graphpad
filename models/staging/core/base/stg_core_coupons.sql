{{
  config(
    materialized = "view",

    tags = ["coupons"]
  )
}}

with source as
(
select *
from {{ source('core','coupons')}}


),

sanitation as
(
  SELECT
  archiveBit as archive_bit
  ,authCode as authcode
  ,brandID as brand_id
  ,description as authcode_description
  ,expDate as exp_date
  ,isFreeUpgradeEnabled as is_free_upgrade_enabled
  ,isInternalUseOnly as is_internal_use_only
  ,isPermanent as is_permanent
  ,isVisible as is_visible
  ,minimumTotalForFreeShipping as minimum_total_for_free_shipping
  ,numberUsesAllowed as number_uses_allowed
  ,numberUsesSoFar as number_uses_so_far
  ,shippingCredit as shipping_credit
  ,sortOrder as sort_order

from source

)

select * from sanitation
