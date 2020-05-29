{{
  config(
    materialized = "view",
    
    tags = ["product", "brand"]
  )
}}

with seat_ceiling as
(
SELECT
a.*
, LEAD(seat_category,1) over (partition by split_part(product_name,' (',1) order by seat_category) as next_seat_category

FROM
{{ref('stg_core_products')}} a


)

, seat_bucketing_concat as
(
SELECT
s.*
, case when seat_category is not null and next_seat_category is null then seat_category || ' - Max'
else seat_category || ' - ' || next_seat_category end as seat_category_range

FROM seat_ceiling s


)

SELECT distinct
a.*
, b.brand_code
, b.brand_domain

, b.brand_name
, b.date_created
, b.is_active
FROM
seat_bucketing_concat a
INNER JOIN {{ref('stg_core_brands')}} b on a.brand_id = b.brand_id
