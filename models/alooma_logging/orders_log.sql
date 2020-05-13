{{
  config(
    materialized='incremental',
    unique_key='_METADATA__TIMESTAMP'
  )
}}

select *

from "GRAPHPAD_DB"."PUBLIC"."ORDERS_LOG"

{% if is_incremental() %}

  where _METADATA__TIMESTAMP > (select max(_METADATA__TIMESTAMP) from {{ this }})

{% endif %}
