{{
  config(
    materialized = "view",
    alias = "product_brand_detail",
    tags = ["product", "brand"]
  )
}}

SELECT distinct
a.*
, b.brand_code
, b.brand_domain
, b.brand_id
, b.brand_name
, b.date_created
, b.is_active
FROM
{{ref('stg_core_products')}} a
INNER JOIN {{ref('stg_core_brands')}} b on a.brand_id = b.brand_id
