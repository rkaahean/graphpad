{{
  config(
    materialized = "view",
  
    tags = ["products"]
  )
}}

with source as
(
select *
from {{ source('core','products')}}


),

sanitation as
(
select
brandID as brand_id
,classID as class_id
,currentVersion as current_version
,description as description
,imagePath as image_path
,internalName as internal_name
,isBundle as is_bundle
,isCouponRequired as is_coupon_required
,isEduPriceEnabled as is_edu_price_enabled
,isNetworkEnabled as is_network_enabled
,isShippingRequired as is_shipping_required
,isSubscription as is_subscription
,isUpgrade as is_upgrade
,isUpgradeEnabled as is_upgrade_enabled
,name as product_name
,productID as product_id
,sequence
,type
,weight
,WinMac as win_mac

from source

)

select * from sanitation
