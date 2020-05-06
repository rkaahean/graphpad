{{
  config(
    materialized = "view",
    alias = "snapgene_subscriptions",
    tags = ["snapgene", "subscriptions"]
  )
}}

select
  regcode,
  case when licensetype = 'trial' then 1 else 0 end as istrial,
  startdate,
  expirationdate,
  licensesgranted as allowedactivations,
  labtype,
  email,
  labname
from {{ source('sg_product', 'regcodes') }}
where
  licensetype in ('subscription', 'network_subscription', 'shared_subscription', 'trial')


{# To do:
- Exclude internal subscriptions by using Seeds
- Add plan id using seeds #}
